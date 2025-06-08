use crate::db::format::{
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType, MAX_KEY_SEQUENCE,
    VALUE_TYPE_FOR_SEEK,
};
use crate::iterator::Iterator;
use crate::options::{Options, ReadOptions};
use crate::storage::Storage;
use crate::table_cache::TableCache;
use crate::util::coding::encode_fixed_64;
use crate::util::comparator::Comparator;
use crate::version::version_edit::FileMetaData;
use crate::version::version_set::total_file_size;
use crate::{Error, Result};
use std::cell::{Cell, RefCell};
use std::cmp::Ordering as CmpOrdering;
use std::fmt;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub mod version_edit;
pub mod version_set;

/// 关于文件搜索（seeking）操作的统计信息。
#[derive(Debug)]
pub struct SeekStats {
    // 文件元数据
    pub file: Arc<FileMetaData>,
    // 文件搜索操作发生在哪一个层级上
    pub level: usize,
}

/// `Version `是不同级别的磁盘表的文件元数据的集合
/// 内存中的DB被写入0级表，压缩将数据从N级迁移到N+1级。这些表将内部键（包括用户键、删除或设置位以及序列号）映射到用户值。
/// 级别0的表是通过增加fileNum进行排序的。
/// 如果两个0级表具有fileNums i和j以及i＜j，则表i中每个内部键的序列号都小于表j的序列号(创建时间上是有序的,序列号较大表示数据更新)。每个0级表中内部键的范围[smallest，largest]可能重叠。
/// 任何非0级别的表都按其内部关键字范围进行排序，并且同一非0级别上的任何两个表都不重叠。
/// 一旦一个内部键被推送到一个较高的层级，任何较低层级中相同用户键的旧版本（序列号较低）就不会被推送到这个或更高的层级
pub struct Version<C: Comparator> {
    vnum: usize, // For debug
    options: Arc<Options<C>>,
    icmp: InternalKeyComparator<C>,

    // 每个level的file元数据按FileMetaData中最小的键排序
    files: Vec<Vec<Arc<FileMetaData>>>,

    // 下一个要根据搜索统计信息压缩的文件
    // TODO: maybe use ShardLock from crossbeam instead.
    //       See https://docs.rs/crossbeam/0.7.1/crossbeam/sync/struct.ShardedLock.html
    //指示下一个需要被压缩的文件
    file_to_compact: RwLock<Option<Arc<FileMetaData>>>,
    file_to_compact_level: AtomicUsize,

    //score低于1表示压缩不是严格必需的，这个字段和下一个字段通常在 finalize 函数中初始化
    compaction_score: f32,
    // 应该被压缩的层级索引,这通常是根据 compaction_score 决定的
    compaction_level: usize,
}

impl<C: Comparator> fmt::Debug for Version<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "vnum: {} ", &self.vnum)?;
        for (level, files) in self.files.iter().enumerate() {
            write!(f, "level {}: [ ", level)?;
            for file in files {
                write!(
                    f,
                    "File {}({}): [{:?}..{:?}], ",
                    file.number, file.file_size, file.smallest, file.largest
                )?;
            }
            writeln!(f, " ]")?;
        }
        Ok(())
    }
}

impl<C: Comparator + 'static> Version<C> {
    //最大level由max_levels设置
    pub fn new(options: Arc<Options<C>>, icmp: InternalKeyComparator<C>) -> Self {
        let max_levels = options.max_levels as usize;
        let mut files = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            files.push(Vec::new());
        }
        Self {
            vnum: 0,
            options,
            icmp,
            files,
            file_to_compact: RwLock::new(None),
            file_to_compact_level: AtomicUsize::new(0),
            compaction_score: 0f32,
            compaction_level: 0,
        }
    }

    /// 按sstables中给定的键逐级搜索值 table_cache 是一个表缓存，用于访问存储文件
    /// 返回 包含可能的值（Vec<u8>）和搜索统计信息（SeekStats）
    pub fn get<S: Storage + Clone + 'static>( &self, options: ReadOptions, key: LookupKey,table_cache: &TableCache<S, C>,) -> Result<(Option<Vec<u8>>, Option<SeekStats>)> {
        // 初始化键和比较器
        let ikey = key.internal_key();
        let ukey = key.user_key();
        let ucmp = &self.icmp.user_comparator;
        //搜索统计信息
        let mut seek_stats = None;
        //将要搜索的文件列表
        let mut files_to_seek = vec![];
        // 遍历各层文件，找到要查找的文件列表
        for (level, files) in self.files.iter().enumerate() {
            // 如果某层文件为空，继续下一层
            if files.is_empty() {
                continue;
            }
            // 对于 0 级，需要考虑文件重叠的问题，检查所有可能包含 ukey 的文件。
            if level == 0 {
                // 重叠 user_key 并按照从最新到最旧的顺序处理它们(序列号大小)，因为最后一个 0 级文件总是有最新的条目。
                for f in files.iter().rev() {
                    if ucmp.compare(ukey, f.largest.user_key()) != CmpOrdering::Greater
                        && ucmp.compare(ukey, f.smallest.user_key()) != CmpOrdering::Less
                    {
                        files_to_seek.push((f, 0));
                    }
                }
            } else {
                // 对于非 0 级，使用二分查找确定 ikey 可能存在的文件。
                // file.largest>=ikey
                let index = find_file(&self.icmp, files, ikey);
                if index >= files.len() {
                    // 没有找到匹配的文件
                } else {
                    let target = &files[index];
                    // 用户键大于或等于文件的最小键 添加到files_to_seek 中
                    if ucmp.compare(ukey, target.smallest.user_key()) != CmpOrdering::Less{
                        files_to_seek.push((target, level));
                    }
                }
            }
        }
        // 按文件编号从大到小排序，以确定访问顺序
        files_to_seek.sort_by(|(a, _), (b, _)| b.number.cmp(&a.number));
        // 遍历排序后的文件，使用 table_cache 来加载并检查数据块。
        for (file, level) in files_to_seek {

            if seek_stats.is_none() {
                // TODO：当 Seek Compaction 触发时，LevelDB 首先确定哪些文件被频繁查询。通常，它会记录第一个或最初几个在查询过程中访问的文件 
                // Seek Compaction，每个文件的 seek miss 次数都有一个阈值，如果超过了这个阈值，那么认为这个文件需要Compact。
                seek_stats = Some(SeekStats {
                    file: file.clone(),
                    level,
                });
            }
            match table_cache.get(
                self.icmp.clone(),
                options,
                ikey,
                file.number,
                file.file_size,
            )? {
                None => continue,
                Some(block_iter) => {
                    let encoded_key = block_iter.key();
                    let value = block_iter.value();
                    match ParsedInternalKey::decode_from(encoded_key) {
                        None => return Err(Error::Corruption("bad internal key".to_owned())),
                        Some(parsed_key) => {
                            // 如果找到与 ukey 相匹配的键，并且数据类型是有效值（ValueType::Value），返回该值。
                            if self
                                .options
                                .comparator
                                .compare(parsed_key.user_key, key.user_key())
                                == CmpOrdering::Equal
                            {
                                match parsed_key.value_type {
                                    ValueType::Value => {
                                        return Ok((Some(value.to_vec()), seek_stats))
                                    }
                                    ValueType::Deletion => return Ok((None, seek_stats)),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok((None, seek_stats))
    }

    /// 该方法 update_stats 的作用是更新 SSTable 文件的查询（seek）统计，并根据统计结果可能将文件标记为需要压缩
    pub fn update_stats(&self, stats: Option<SeekStats>) -> bool {
        if let Some(ss) = stats {
            // 更新文件的 allowed_seeks 字段
            let old = ss
                .file
                .allowed_seeks
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                    Some(if v > 0 { v - 1 } else { 0 })
                })
                .unwrap();
            let mut file_to_compact = self.file_to_compact.write().unwrap();
            // 如果 file_to_compact 当前没有标记任何文件为待压缩并且 old 的值（即之前的 allowed_seeks 值）为1
            if file_to_compact.is_none() && old == 1 {
                // 设置 file_to_compact 为当前文件，并更新 file_to_compact_level 为文件所在的层级
                *file_to_compact = Some(ss.file);
                self.file_to_compact_level
                    .store(ss.level, Ordering::Release);
                return true;
            }
        }
        false
    }

    /// version是否需要压缩 compaction_score 或者有标记文件
    pub fn needs_compaction(&self) -> bool {
        self.compaction_score > 1.0 || self.file_to_compact.read().unwrap().is_some()
    }

    /// 查看每个level中的文件数量
    pub fn level_summary(&self) -> String {
        // files[...]
        let mut s = String::from("files[ ");
        let summary = self.files.iter().fold(String::new(), |mut acc, files| {
            acc.push_str(format!("{} ", files.len()).as_str());
            acc
        });
        s.push_str(summary.as_str());
        s.push(']');
        s
    }

    /// 指示新的 MemTable 压缩结果应该放在哪个层级
    /// 这个决策基于是否存在键范围重叠以及重叠程度的判断(下一个层级有重叠或者祖父层级的重叠文件大小>max_grandparent_overlap_bytes)
    pub fn pick_level_for_memtable_output(
        &self,
        smallest_ukey: &[u8],
        largest_ukey: &[u8],
    ) -> usize {
        let mut level = 0;
        // 调用 overlap_in_level 方法检查在层级 0 是否存在与给定键范围重叠的文件
        // 如果没有重叠，考虑将数据推送到更高的层级。
        if !self.overlap_in_level(level, Some(smallest_ukey), Some(largest_ukey)) {
            let smallest_ikey =
                InternalKey::new(smallest_ukey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
            let largest_ikey = InternalKey::new(largest_ukey, 0, ValueType::Deletion);
            // 循环检查每个层级，直到达到配置的最大 MemTable 压缩层级 max_mem_compact_level。
            while level < self.options.max_mem_compact_level {
                // 检查下一层级是否有重叠。如果有，停止提升层级。
                if self.overlap_in_level(level + 1, Some(smallest_ukey), Some(largest_ukey)) {
                    break;
                }
                if level + 2 < self.options.max_levels as usize {
                    // 检查在祖父层级（当前层级加 2）的文件重叠总大小
                    let overlaps = self.get_overlapping_inputs(
                        level + 2,
                        Some(&smallest_ikey),
                        Some(&largest_ikey),
                    );
                    // 如果重叠的文件总大小超过了配置的最大值 max_grandparent_overlap_bytes，则停止层级提升
                    if total_file_size(&overlaps) > self.options.max_grandparent_overlap_bytes() {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    // 用于计算 LSM 树的每个level的score得分，并确定哪个level最需要进行压缩
    pub fn finalize(&mut self) {
        // pre-computed best level for next compaction
        let mut best_level = 0;
        let mut best_score = 0.0;
        //循环遍历所有层级（从0开始到最大层级数）
        for level in 0..self.options.max_levels as usize {
            let score = {
                // （level-0）特殊处理
                // 使用文件数量与配置的阈值（l0_compaction_threshold）比较，而不是使用文件的总大小
                // 因为第0层的文件在每次读取时都需要合并，过多的文件会导致性能问题。
                // level 0的文件之间，key可能是交叉重叠的，因此不希望level 0的文件数特别多
                // 1. 内存表数据比较小的时候，如果使用size来限制，那么level 0的文件数可能太多。
                // 2. 如果内存表数据过大，使用固定大小的size 来限制level 0的话，可能算出来的level 0的文件数又太少，每个文件变得更大，触发 level 0 compaction的情况发生的又太频繁
                if level == 0 {
                    self.files[level].len() as f64 / self.options.l0_compaction_threshold as f64
                } else {
                    //其他层级的得分计算则基于文件总大小与该level允许的最大字节量（由 self.options.max_bytes_for_level(level) 给出）的比例
                    let level_bytes = total_file_size(self.files[level].as_ref());
                    level_bytes as f64 / self.options.max_bytes_for_level(level) as f64
                }
            };
            if score > best_score {
                best_score = score;
                best_level = level;
            }
        }
        self.compaction_level = best_level;
        self.compaction_score = best_score as f32;
    }

    /// Returns `icmp`
    #[inline]
    pub fn comparator(&self) -> InternalKeyComparator<C> {
        self.icmp.clone()
    }

    /// 返回给level中的文件slice
    ///
    /// # Panic
    ///
    /// `level` is out bound of `files`
    #[inline]
    pub fn get_level_files(&self, level: usize) -> &[Arc<FileMetaData>] {
        assert!(
            level < self.files.len(),
            "[version] invalid level {}, the max level is {}",
            level,
            self.options.max_levels - 1
        );
        self.files[level].as_slice()
    }

    /// 此函数的目的是为每个与给定键重叠的文件执行特定的操作（通过 Call `func(level, file)`），并且按照从最新到最旧的顺序处理
    pub fn for_each_overlapping(
        &self,
        user_key: &[u8],
        internal_key: &[u8],
        mut func: Box<dyn FnMut(usize, Arc<FileMetaData>) -> bool>,
    ) {
        let ucmp = &self.icmp.user_comparator;
        for (level, files) in self.files.iter().enumerate() {
            // 处理 Level 0
            if level == 0 {
                let mut target_files = vec![];
                // 寻找level0重叠的文件
                for f in files.iter() {
                    if ucmp.compare(user_key, f.smallest.user_key()) != CmpOrdering::Less
                        && ucmp.compare(user_key, f.largest.user_key()) != CmpOrdering::Greater
                    {
                        target_files.push(f);
                    }
                }
                if !target_files.is_empty() {
                    target_files.sort_by(|a, b| b.number.cmp(&a.number))
                }
                //对每个文件执行func回调
                for target_file in target_files {
                    if !func(0, target_file.clone()) {
                        return;
                    }
                }
            } else {
                if files.is_empty() {
                    continue;
                }
                let index = find_file(&self.icmp, self.files[level].as_slice(), internal_key);
                if index >= files.len() {
                    // not found
                } else {
                    let target = files[index].clone();
                    if ucmp.compare(user_key, target.smallest.data()) != CmpOrdering::Less
                        && !func(level, target)
                    {
                        return;
                    }
                }
            }
        }
    }

    /// 用于记录在特定内部键上读取的样本，并根据读取模式判断是否需要触发新的压缩操作
    /// 如果可能需要触发新的压缩，则返回 true
    pub fn record_read_sample(&self, internal_key: &[u8]) -> bool {
        if let Some(pkey) = ParsedInternalKey::decode_from(internal_key) {
            let stats = Rc::new(Cell::new(None));
            let matches = Rc::new(RefCell::new(0));
            let stats_clone = stats.clone();
            let matches_clone = matches.clone();
            // 遍历重叠文件 执行func
            // 增加 matches 计数。
            // 如果是第一个匹配的文件，记录其文件元数据和层级信息到 stats。
            // 如果已有两个匹配，返回 false 停止遍历。
            self.for_each_overlapping(
                pkey.user_key,
                internal_key,
                Box::new(move |level, file| {
                    *matches_clone.borrow_mut() += 1;
                    if *matches_clone.borrow() == 1 {
                        // Remember first match
                        stats_clone.set(Some(SeekStats { file, level }));
                    }
                    *matches_clone.borrow() < 2
                }),
            );

            // 检查是否至少有两个重叠文件，因为压缩通常需要跨文件合并
            if *matches.borrow() >= 2 {
                if let Ok(s) = Rc::try_unwrap(stats) {
                    return self.update_stats(s.into_inner());
                }
            }
        }
        false
    }

    /// 检查在给定层级（level）上是否有任何文件与指定的键范围 [smallest_ukey, largest_ukey] 重叠
    /// `smallest_ukey` 是 `None` 表示比所有 DB 的键都小的键。
    /// `largest_ukey`是`None`，表示比DB 所有键都大的键
    pub fn overlap_in_level(
        &self,
        level: usize,
        smallest_ukey: Option<&[u8]>,
        largest_ukey: Option<&[u8]>,
    ) -> bool {
        some_file_overlap_range(
            &self.icmp,
            level > 0,
            &self.files[level],
            smallest_ukey,
            largest_ukey,
        )
    }

    /// 返回类型为 u64，表示内部键（ikey）在所有存储文件中的大致字节偏移量。
    /// 近似的偏移量，因为level0是局部有序的而不是全局
    pub fn approximate_offset_of<S: Storage + Clone>(&self, ikey: &InternalKey, table_cache: &TableCache<S, C>) -> u64 {
        let mut result = 0;
        // 遍历文件层级和文件
        for (level, files) in self.files.iter().enumerate() {
            for f in files {
                // 判断 ikey 与文件中最大和最小键的关系
                if self.icmp.compare(f.largest.data(), ikey.data()) != CmpOrdering::Greater {
                    // 如果 ikey 大于文件的最大键，则累加该文件的大小到 result
                    result += f.file_size;
                } else if self.icmp.compare(f.smallest.data(), ikey.data()) == CmpOrdering::Greater
                {
                    // 如果 ikey 小于文件的最小键，并且该层级大于0
                    if level > 0 {
                        // 中断当前层的处理 因为后续文件也不会包含 ikey
                        break;
                    }
                } else {
                    // 如果 ikey 在文件的键范围内，使用 table_cache 访问该文件并计算 ikey 在文件内的大致偏移量，然后累加到 result。
                    if let Ok(table) =
                        table_cache.find_table(self.icmp.clone(), f.number, f.file_size)
                    {
                        result += table.approximate_offset_of(self.icmp.clone(), ikey.data());
                    }
                }
            }
        }
        result
    }

    // “begin”和“end”都是“InternalKey”，但只比较userkey
    // level: 要检查的层级。
    // begin: 起始内部键的可选引用，如果为 None，表示范围的开始是无限小。
    // end: 结束内部键的可选引用，如果为 None，表示范围的结束是无限大。
    // 获取level中所有与给定范围重叠的文件用于合并， level0无序，如果找到重叠文件合并，其他文件也会存在与合并文件重叠的地方
    fn get_overlapping_inputs(
        &self,
        level: usize,
        begin: Option<&InternalKey>,
        end: Option<&InternalKey>,
    ) -> Vec<Arc<FileMetaData>> {
        let cmp = &self.icmp.user_comparator;
        // 提取 begin 和 end 内部键中的用户键部分
        let mut user_begin = begin.map(|ik| ik.user_key());
        let mut user_end = end.map(|ik| ik.user_key());
        let mut result = vec![];
        let mut need_restart = true;

        while need_restart {
            need_restart = false;
            for file in self.files[level].iter() {
                let file_begin = file.smallest.user_key();
                let file_end = file.largest.user_key();
                // 如果文件完全位于指定范围之前或之后，则跳过该文件。
                if user_begin.is_some() && cmp.compare(file_end, user_begin.unwrap()) == CmpOrdering::Less {
                    continue;
                }
                if user_end.is_some() && cmp.compare(file_begin, user_end.unwrap()) == CmpOrdering::Greater {
                    continue;
                }

                // 处理 Level 0 的特殊情况，其中文件可能相互重叠，需要重新计算范围并重新搜索。
                if level == 0 {
                    // 对于level 0，sstable文件可能相互有重叠
                    // 是否范围更大，如果是则扩展范围重新开始搜索
                    // 文件最小键比已知搜索范围的最小键还要小，将搜索范围向前扩展到这个文件的开始键
                    let expand_begin = user_begin.is_some() && cmp.compare(file_begin, user_begin.unwrap()) == CmpOrdering::Less;
                    let expand_end = user_end.is_some() && cmp.compare(file_end, user_end.unwrap()) == CmpOrdering::Greater;

                    if expand_begin || expand_end {
                        need_restart = true;
                        if expand_begin {
                            user_begin = Some(file_begin);
                        }
                        if expand_end {
                            user_end = Some(file_end);
                        }
                        result.clear();
                        break;
                    }
                }
                //如果文件与范围重叠
                result.push(file.clone());
            }
        }
        result
    }
}

// 二分搜索算法在有序文件集中查找第一个包含大于或等于给定内部键（ikey）的文件
fn find_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    files: &[Arc<FileMetaData>],
    ikey: &[u8],
) -> usize {
    let mut left = 0_usize;
    let mut right = files.len();
    while left < right {
        let mid = (left + right) / 2;
        let f = &files[mid];
        if icmp.compare(f.largest.data(), ikey) == CmpOrdering::Less {
            // key  "mid.largest" < "target"
            left = mid + 1;
        } else {
            // Key "mid.largest" >= "target"
            right = mid;
        }
    }
    right
}

// 用来判断在给定的文件集合中是否有任何文件与指定的键范围重叠
// disjoint指示文件的范围是否不重叠 如果为 true，则文件范围在同一层中不相交(非level0)，可以使用二分搜索优化查找
fn some_file_overlap_range<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    disjoint: bool,
    files: &[Arc<FileMetaData>],
    smallest_ukey: Option<&[u8]>,
    largest_ukey: Option<&[u8]>,
) -> bool {
    if !disjoint {
        // 需要遍历每个文件并检查是否与给定范围重叠。
        for file in files {
            if key_is_after_file(icmp, file, smallest_ukey)
                || key_is_before_file(icmp, file, largest_ukey)
            {
                // 没有重叠
                continue;
            } else {
                return true;
            }
        }
        return false;
    }
    // 二分查找每个文件
    let index = {
        if let Some(s_ukey) = smallest_ukey {
            let smallest_ikey = InternalKey::new(s_ukey, MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK);
            // file最大键 >= smallest_ikey
            find_file(icmp, files, smallest_ikey.data())
        } else {
            0
        }
    };
    if index >= files.len() {
        // 已超出所有文件的范围
        return false;
    }
    // 检查最大键是否重叠  文件最小键<=largest_ukey
    !key_is_before_file(icmp, &files[index], largest_ukey)
}

// 检查一个ukey是否位于文件包含的键范围的末端之后
fn key_is_after_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    file: &Arc<FileMetaData>,
    ukey: Option<&[u8]>,
) -> bool {
    ukey.is_some()
        && icmp
            .user_comparator
            .compare(ukey.unwrap(), file.largest.user_key())
            == CmpOrdering::Greater
}

// 检查一个ukey是否位于文件包含的键范围的开始之前。
fn key_is_before_file<C: Comparator>(
    icmp: &InternalKeyComparator<C>,
    file: &Arc<FileMetaData>,
    ukey: Option<&[u8]>,
) -> bool {
    ukey.is_some()
        && icmp
            .user_comparator
            .compare(ukey.unwrap(), file.smallest.user_key())
            == CmpOrdering::Less
}

/// 16个字节
pub const FILE_META_LENGTH: usize = 2 * mem::size_of::<u64>();


/// 每个level中的文件的迭代器
/// key() 是文件中出现的最大键，
/// value() 长度为 16 的字节数组 用于存储文件编号(8byte)+文件大小(8byte)的编码值
/// 编码使用`encode_fixed_u64`
pub struct LevelFileNumIterator<C: Comparator> {
    files: Vec<Arc<FileMetaData>>,
    icmp: InternalKeyComparator<C>,
    index: usize,
    value_buf: [u8; 16],
}

impl<C: Comparator + 'static> LevelFileNumIterator<C> {
    pub fn new(icmp: InternalKeyComparator<C>, files: Vec<Arc<FileMetaData>>) -> Self {
        let index = files.len();
        Self {
            files,
            icmp,
            index,
            value_buf: [0; 16],
        }
    }

    #[inline]
    fn fill_value_buf(&mut self) {
        if self.valid() {
            let file = &self.files[self.index];
            encode_fixed_64(&mut self.value_buf, file.number);
            encode_fixed_64(&mut self.value_buf[8..], file.file_size);
        }
    }

    fn valid_or_panic(&self) {
        assert!(self.valid(), "[level file num iterator] out of bounds")
    }
}

impl<C: Comparator + 'static> Iterator for LevelFileNumIterator<C> {
    fn valid(&self) -> bool {
        self.index < self.files.len()
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
        self.fill_value_buf();
    }

    fn seek_to_last(&mut self) {
        if self.files.is_empty() {
            self.index = 0;
        } else {
            self.index = self.files.len() - 1;
        }
        self.fill_value_buf();
    }

    fn seek(&mut self, target: &[u8]) {
        let index = find_file(&self.icmp, self.files.as_slice(), target);
        self.index = index;
        self.fill_value_buf();
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.index += 1;
        self.fill_value_buf();
    }

    fn prev(&mut self) {
        self.valid_or_panic();
        if self.index == 0 {
            // marks as invalid
            self.index = self.files.len();
        } else {
            self.index -= 1;
            self.fill_value_buf();
        }
    }

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        self.files[self.index].largest.data()
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        &self.value_buf
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod find_file_tests {
    use super::*;
    use crate::db::format::{InternalKey, InternalKeyComparator, ValueType};
    use crate::util::comparator::BytewiseComparator;

    #[derive(Default)]
    struct FindFileTest {
        // Indicate whether file ranges are already disjoint
        overlapping: bool,
        files: Vec<Arc<FileMetaData>>,
        cmp: InternalKeyComparator<BytewiseComparator>,
    }

    impl FindFileTest {
        fn add(&mut self, smallest: &str, largest: &str) {
            let mut file = FileMetaData::default();
            file.number = self.files.len() as u64 + 1;
            file.smallest = InternalKey::new(smallest.as_bytes(), 100, ValueType::Value);
            file.largest = InternalKey::new(largest.as_bytes(), 100, ValueType::Value);
            self.files.push(Arc::new(file));
        }

        fn add_with_seq(&mut self, smallest: (&str, u64), largest: (&str, u64)) {
            let mut file = FileMetaData::default();
            file.number = self.files.len() as u64 + 1;
            file.smallest = InternalKey::new(smallest.0.as_bytes(), smallest.1, ValueType::Value);
            file.largest = InternalKey::new(largest.0.as_bytes(), largest.1, ValueType::Value);
            self.files.push(Arc::new(file));
        }

        fn find(&self, key: &str) -> usize {
            let ikey = InternalKey::new(key.as_bytes(), 100, ValueType::Value);
            find_file(&self.cmp, &self.files, ikey.data())
        }

        fn overlaps(&self, smallest: Option<&str>, largest: Option<&str>) -> bool {
            some_file_overlap_range(
                &self.cmp,
                !self.overlapping,
                &self.files,
                smallest.map(|s| s.as_bytes()),
                largest.map(|s| s.as_bytes()),
            )
        }
    }

    #[test]
    fn test_empty_file_set() {
        let t = FindFileTest::default();
        assert_eq!(0, t.find("foo"));
        assert!(!t.overlaps(Some("a"), Some("z")));
        assert!(!t.overlaps(None, Some("z")));
        assert!(!t.overlaps(Some("a"), None));
        assert!(!t.overlaps(None, None));
    }

    #[test]
    fn test_find_file_with_single_file() {
        let mut t = FindFileTest::default();
        t.add("p", "q");
        // Find tests
        for (expected, input) in vec![(0, "a"), (0, "p"), (0, "p1"), (0, "q"), (1, "q1"), (1, "z")]
        {
            assert_eq!(expected, t.find(input), "input {}", input);
        }
        // Overlap tests
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("a"), Some("b"))),
            (false, (Some("z1"), Some("z2"))),
            (true, (Some("a"), Some("p"))),
            (true, (Some("a"), Some("q"))),
            (true, (Some("p"), Some("p1"))),
            (true, (Some("p"), Some("q"))),
            (true, (Some("p1"), Some("p2"))),
            (true, (Some("p1"), Some("z"))),
            (true, (Some("q"), Some("q"))),
            (true, (Some("q"), Some("q1"))),
            (false, (None, Some("j"))),
            (false, (Some("r"), None)),
            (true, (None, Some("p"))),
            (true, (None, Some("p1"))),
            (true, (Some("q"), None)),
            (true, (None, None)),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_find_files_with_various_files() {
        let mut t = FindFileTest::default();
        for (start, end) in vec![
            ("150", "200"),
            ("200", "250"),
            ("300", "350"),
            ("400", "450"),
        ] {
            t.add(start, end);
        }
        // Find tests
        for (expected, input) in vec![
            (0, "100"),
            (0, "150"),
            (0, "151"),
            (0, "199"),
            (0, "200"),
            (1, "201"),
            (1, "249"),
            (1, "250"),
            (2, "251"),
            (2, "301"),
            (2, "350"),
            (3, "351"),
            (4, "451"),
        ] {
            assert_eq!(expected, t.find(input), "input {}", input);
        }
        // Overlap tests
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("100"), Some("149"))),
            (false, (Some("251"), Some("299"))),
            (false, (Some("451"), Some("500"))),
            (false, (Some("351"), Some("399"))),
            (true, (Some("100"), Some("150"))),
            (true, (Some("100"), Some("200"))),
            (true, (Some("100"), Some("300"))),
            (true, (Some("100"), Some("400"))),
            (true, (Some("100"), Some("500"))),
            (true, (Some("375"), Some("400"))),
            (true, (Some("450"), Some("450"))),
            (true, (Some("450"), Some("500"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_multiple_null_boundaries() {
        let mut t = FindFileTest::default();
        for (start, end) in vec![
            ("150", "200"),
            ("200", "250"),
            ("300", "350"),
            ("400", "450"),
        ] {
            t.add(start, end);
        }
        for (expected, (lhs, rhs)) in vec![
            (false, (None, Some("149"))),
            (false, (Some("451"), None)),
            (true, (None, None)),
            (true, (None, Some("150"))),
            (true, (None, Some("199"))),
            (true, (None, Some("200"))),
            (true, (None, Some("201"))),
            (true, (None, Some("400"))),
            (true, (None, Some("800"))),
            (true, (Some("100"), None)),
            (true, (Some("200"), None)),
            (true, (Some("449"), None)),
            (true, (Some("450"), None)),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_overlap_sequence_check() {
        let mut t = FindFileTest::default();
        t.add_with_seq(("200", 5000), ("200", 300));
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("199"), Some("199"))),
            (false, (Some("201"), Some("300"))),
            (true, (Some("200"), Some("200"))),
            (true, (Some("190"), Some("200"))),
            (true, (Some("200"), Some("210"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }

    #[test]
    fn test_overlapping_files() {
        let mut t = FindFileTest::default();
        t.overlapping = true;
        t.add("150", "600");
        t.add("400", "500");
        for (expected, (lhs, rhs)) in vec![
            (false, (Some("100"), Some("149"))),
            (false, (Some("601"), Some("700"))),
            (true, (Some("100"), Some("150"))),
            (true, (Some("100"), Some("200"))),
            (true, (Some("100"), Some("300"))),
            (true, (Some("100"), Some("400"))),
            (true, (Some("100"), Some("500"))),
            (true, (Some("375"), Some("400"))),
            (true, (Some("450"), Some("450"))),
            (true, (Some("450"), Some("500"))),
            (true, (Some("450"), Some("700"))),
            (true, (Some("600"), Some("700"))),
        ] {
            assert_eq!(expected, t.overlaps(lhs, rhs))
        }
    }
}
