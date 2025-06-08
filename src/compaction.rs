use crate::db::format::{InternalKey, InternalKeyComparator};
use crate::error::Result;
use crate::iterator::{ConcatenateIterator, KMergeIter};
use crate::options::{Options, ReadOptions};
use crate::sstable::table::TableBuilder;
use crate::storage::{File, Storage};
use crate::table_cache::TableCache;
use crate::util::comparator::Comparator;
use crate::version::version_edit::{FileMetaData, VersionEdit};
use crate::version::version_set::{total_file_size, FileIterFactory, SSTableIters};
use crate::version::{LevelFileNumIterator, Version};
use crossbeam_channel::Sender;
use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

/// Information for a manual compaction
#[derive(Clone)]
pub struct ManualCompaction {
    pub level: usize,
    pub done: Sender<Result<()>>,
    pub begin: Option<InternalKey>, // None means beginning of key range
    pub end: Option<InternalKey>,   // None means end of key range
}

// A helper struct representing all the files to be compacted.
// All the files in `base` or `parent` must be sorted by key range.
#[derive(Default, Debug)]
pub struct CompactionInputs {
    // level n files
    pub base: Vec<Arc<FileMetaData>>,
    // level n+1 files
    pub parent: Vec<Arc<FileMetaData>>,
}

impl CompactionInputs {
    /// Add a file info to base level files
    #[inline]
    pub fn add_base(&mut self, f: Arc<FileMetaData>) {
        self.base.push(f);
        self.base.iter();
    }

    fn iter_all(&self) -> impl Iterator<Item = &Arc<FileMetaData>> {
        self.base.iter().chain(self.parent.iter())
    }

    #[inline]
    pub fn desc_base_files(&self) -> String {
        self.base
            .iter()
            .map(|f| f.number.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    #[inline]
    pub fn desc_parent_files(&self) -> String {
        self.parent
            .iter()
            .map(|f| f.number.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

#[derive(Debug)]
pub enum CompactionReason {
    MaxSize,
    SeekLimit,
    Manual,
}

// 封装了数据库压缩操作中的各种信息
pub struct Compaction<F: File, C: Comparator> {
    options: Arc<Options<C>>,
    // 触发压缩的原因
    pub reason: CompactionReason,
    // 目标压缩level层级
    pub level: usize,
    // 参与压缩的版本信息
    pub input_version: Option<Arc<Version<C>>>,
    // 压缩结果的总结
    pub edit: VersionEdit,
    //参与压缩的输入文件，包含了要被压缩的当前级别（level n）和下一级别（level n + 1）的文件，这些文件在使用前已经被排序。
    pub inputs: CompactionInputs,

    // 与压缩操作相关的祖父级文件（即level n + 2级别的文件）。用于检测与下下个级别的文件重叠情况
    pub grand_parents: Vec<Arc<FileMetaData>>,
    // 当前处理到的祖父级文件索引。
    pub grand_parent_index: usize,

    // 在should_stop_before方法中使用，用于标记是否遇到了需要特别处理的键。
    pub seen_key: bool,
    // 当前输出文件与祖父级文件重叠的字节数
    pub overlapped_bytes: u64,

    // Sequence numbers less than this are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S
    // 最老的存活快照序列号
    // 如果S <=smallest_snapshot 则可以删除序列号 < S 的同一键的所有条目
    pub oldest_snapshot_alive: u64,

    // 压缩操作生成的所有输出文件的元数据。
    pub outputs: Vec<FileMetaData>,

    // 当前用于输出sst文件的表构建器
    // 在达到某些条件时（如文件大小达到阈值），会创建新的构建器。`should_stop_before`
    pub builder: Option<TableBuilder<InternalKeyComparator<C>, F>>,

    // 已经写入的总字节数
    pub total_bytes: u64,
}

impl<O: File, C: Comparator + 'static> Compaction<O, C> {
    pub fn new(options: Arc<Options<C>>, level: usize, reason: CompactionReason) -> Self {
        let max_levels = options.max_levels;
        Self {
            reason,
            options,
            level,
            input_version: None,
            edit: VersionEdit::new(max_levels),
            inputs: CompactionInputs::default(),
            grand_parents: vec![],
            grand_parent_index: 0,
            seen_key: false,
            overlapped_bytes: 0,
            oldest_snapshot_alive: 0,
            outputs: vec![],
            builder: None,
            total_bytes: 0,
        }
    }

    /// 主要通过检查当前层级的输入文件以及与父层级和祖父层级文件的重叠情况，决定是否可以直接将文件移动到下一个层级，而无需进行合并或拆分
    // TODO: improve this to satisfy more complicate moving
    pub fn is_trivial_move(&self) -> bool {
        self.inputs.base.len() == 1
            && self.inputs.parent.is_empty()
            && total_file_size(&self.grand_parents) <= self.options.max_grandparent_overlap_bytes()
    }

    /// Create an iterator that reads over all the compaction input tables with merged order.
    /// We produce different iter for tables in level0 and level >0 :
    ///     level 0:  Since key ranges might be overlapped with each other, we generate
    ///              a table iterator over every single level 0 sst file
    ///     level > 0: a `ConcatenateIterator` for all the sst file in this level
    ///
    /// Entry format:
    ///     key: internal key
    ///     value: value of user key
    pub fn new_input_iterator<S: Storage + Clone + 'static>(
        &self,
        icmp: InternalKeyComparator<C>,
        table_cache: TableCache<S, C>,
    ) -> Result<KMergeIter<SSTableIters<S, C>>> {
        let read_options = ReadOptions {
            verify_checksums: self.options.paranoid_checks,
            fill_cache: false,
            snapshot: None,
        };
        // Level-0 files have to be merged together so we generate a merging iterator includes iterators for each level 0 file.
        // For other levels, we will make a concatenating iterator per level.
        let mut level0 = Vec::with_capacity(self.inputs.base.len() + 1);
        let mut leveln = Vec::with_capacity(2);
        if self.level == 0 {
            for file in self.inputs.base.iter() {
                debug!(
                    "new level {} table iter: number {}, file size {}, [{:?} ... {:?}]",
                    self.level, file.number, file.file_size, file.smallest, file.largest
                );
                level0.push(table_cache.new_iter(
                    icmp.clone(),
                    read_options,
                    file.number,
                    file.file_size,
                )?);
            }
        } else {
            for f in &self.inputs.base {
                debug!(
                    "new level {} table iter: number {}, file size {}, [{:?} ... {:?}]",
                    self.level, f.number, f.file_size, f.smallest, f.largest
                );
            }
            let origin = LevelFileNumIterator::new(icmp.clone(), self.inputs.base.clone());
            let factory = FileIterFactory::new(icmp.clone(), read_options, table_cache.clone());
            leveln.push(ConcatenateIterator::new(origin, factory));
        }
        if !self.inputs.parent.is_empty() {
            for f in &self.inputs.parent {
                debug!(
                    "new level {} table iter: number {}, file size {}, [{:?} ... {:?}]",
                    self.level + 1,
                    f.number,
                    f.file_size,
                    f.smallest,
                    f.largest
                );
            }
            let origin = LevelFileNumIterator::new(icmp.clone(), self.inputs.parent.clone());
            let factory = FileIterFactory::new(icmp.clone(), read_options, table_cache);
            leveln.push(ConcatenateIterator::new(origin, factory));
        }

        let iter = KMergeIter::new(SSTableIters::new(icmp, level0, leveln));
        Ok(iter)
    }

    /// Returns true iff we should stop building the current output
    /// before processing `ikey` for too much overlapping with grand parents
    pub fn should_stop_before(&mut self, ikey: &[u8], icmp: &InternalKeyComparator<C>) -> bool {
        // `seen_key` guarantees that we should continue checking for next `ikey`
        // no matter whether the first `ikey` overlaps with grand parents
        while self.grand_parent_index < self.grand_parents.len()
            && icmp.compare(
                ikey,
                self.grand_parents[self.grand_parent_index].largest.data(),
            ) == CmpOrdering::Greater
        {
            if self.seen_key {
                self.overlapped_bytes += self.grand_parents[self.grand_parent_index].file_size
            }
            self.grand_parent_index += 1;
        }
        self.seen_key = true;
        if self.overlapped_bytes > self.options.max_grandparent_overlap_bytes() {
            // Too much overlap for current output, start new output
            self.overlapped_bytes = 0;
            return true;
        }
        false
    }

    /// Reports whether it is guaranteed that there are no
    /// key/value pairs at c.level+2 or higher that have the user key ukey.
    pub fn key_exist_in_deeper_level(&mut self, ukey: &[u8]) -> bool {
        let v = self.input_version.as_ref().unwrap();
        let ucmp = &self
            .input_version
            .as_ref()
            .unwrap()
            .comparator()
            .user_comparator;
        let max_levels = self.options.max_levels;
        if self.level + 2 < max_levels {
            for level in self.level + 2..max_levels {
                for f in v.get_level_files(level) {
                    if ucmp.compare(ukey, f.largest.user_key()) != CmpOrdering::Greater {
                        if ucmp.compare(ukey, f.smallest.user_key()) != CmpOrdering::Less {
                            return true;
                        }
                        // For levels above level 0, the files within a level are in
                        // increasing ikey order, so we can break early.
                        break;
                    }
                }
            }
        }
        false
    }

    /// Apply deletion for current inputs and current output files to the edit
    pub fn apply_to_edit(&mut self) {
        for f in self.inputs.base.iter() {
            self.edit.delete_file(self.level, f.number)
        }
        for f in self.inputs.parent.iter() {
            self.edit.delete_file(self.level + 1, f.number)
        }
        for output in self.outputs.drain(..) {
            self.edit
                .file_delta
                .new_files
                .push((self.level + 1, output))
        }
    }

    /// Calculate the read bytes
    #[inline]
    pub fn bytes_read(&self) -> u64 {
        self.inputs
            .iter_all()
            .fold(0, |sum, file| sum + file.file_size)
    }

    /// Calculate the written bytes
    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.outputs.iter().fold(0, |sum, file| sum + file.number)
    }
}

/// Returns the minimal range that covers all entries in `files`
pub fn base_range<'a, C: Comparator>(
    files: &'a [Arc<FileMetaData>],
    level: usize,
    icmp: &InternalKeyComparator<C>,
) -> (&'a InternalKey, &'a InternalKey) {
    assert!(
        !files.is_empty(),
        "[compaction] the input[0] shouldn't be empty when trying to get covered range"
    );
    if level == 0 {
        // level 0 files are possible to overlaps with each other
        let mut smallest = &files.first().unwrap().smallest;
        let mut largest = &files.first().unwrap().largest;
        for f in files.iter().skip(1) {
            if icmp.compare(f.smallest.data(), smallest.data()) == CmpOrdering::Less {
                smallest = &f.smallest;
            }
            if icmp.compare(f.largest.data(), largest.data()) == CmpOrdering::Greater {
                largest = &f.largest;
            }
        }
        (smallest, largest)
    } else {
        // no overlapping in level > 0 and file is ordered by smallest key
        (
            &files.first().unwrap().smallest,
            &files.last().unwrap().largest,
        )
    }
}

/// Returns the minimal range that covers all key ranges in `current_l_files` and `next_l_files`
/// `current_l_files` means current level files to be compacted
/// `next_l_files` means next level files to be compacted
pub fn total_range<'a, C: Comparator>(
    current_l_files: &'a [Arc<FileMetaData>],
    next_l_files: &'a [Arc<FileMetaData>],
    level: usize,
    icmp: &InternalKeyComparator<C>,
) -> (&'a InternalKey, &'a InternalKey) {
    let (mut smallest, mut largest) = base_range(current_l_files, level, icmp);
    if !next_l_files.is_empty() {
        let first = next_l_files.first().unwrap();
        if icmp.compare(first.smallest.data(), smallest.data()) == CmpOrdering::Less {
            smallest = &first.smallest
        }
        let last = next_l_files.last().unwrap();
        if icmp.compare(last.largest.data(), largest.data()) == CmpOrdering::Greater {
            largest = &last.largest
        }
    }
    (smallest, largest)
}
/// A helper struct for recording the statistics in compactions
#[derive(Debug)]
pub struct CompactionStats {
    // The microseconds this compaction takes
    pub micros: u64,
    /// The data size read by this compaction
    pub bytes_read: u64,
    /// The data size created in new generated SSTables
    pub bytes_written: u64,
}
