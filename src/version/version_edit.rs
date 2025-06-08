use crate::db::format::InternalKey;
use crate::util::collection::HashSet;
use crate::util::varint::{VarintU32, VarintU64};
use crate::version::version_edit::Tag::{
    CompactPointer, Comparator, DeletedFile, LastSequence, LogNumber, NewFile, NextFileNumber,
    PrevLogNumber, Unknown,
};
use crate::{Error, Options, Result};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};

// VersionEdit 磁盘操作类型标记
// Tag 8 is no longer used.
enum Tag {
    Comparator = 1, //标记用于存储自定义比较器的名称
    LogNumber = 2, //标记用于存储日志文件编号
    NextFileNumber = 3, //标记用于存储下一个文件编号
    LastSequence = 4, //标记用于存储数据库的最后一个序列号
    CompactPointer = 5, //标记用于存储压缩操作的相关信息
    DeletedFile = 6,  //标记用于记录已删除的文件的信息
    NewFile = 7,    //标记用于记录新添加的文件的信息
    // 8 was used for large value refs
    PrevLogNumber = 9,  //标记用于存储之前的日志文件编号
    Unknown, // unknown tag
}

impl From<u32> for Tag {
    fn from(i: u32) -> Self {
        match i {
            1 => Tag::Comparator,
            2 => Tag::LogNumber,
            3 => Tag::NextFileNumber,
            4 => Tag::LastSequence,
            5 => Tag::CompactPointer,
            6 => Tag::DeletedFile,
            7 => Tag::NewFile,
            9 => Tag::PrevLogNumber,
            _ => Tag::Unknown,
        }
    }
}

///代表一个level中的 sst 表创建后不会更改。
#[derive(Debug)]
pub struct FileMetaData {
    // Seeks allowed until compaction
    //
    // 通过追踪查找失败的次数来决定何时对文件进行压缩（compaction）
    // 如果在某个层级的文件频繁发生查找失败，这表明该层级的文件与下一层级（level n + 1）的文件有重度重叠。
    // 在这种情况下，这些查找失败可以被视为一种信号，表明需要对这些文件进行压缩操作。
    // 系统为每个文件维护一个“allowed_seeks”计数器。
    // 这个计数器跟踪在触发压缩之前允许的查找失败次数。一旦查找失败次数达到预设的阈值，系统将自动触发压缩操作。
    pub allowed_seeks: AtomicUsize,
    // 文件大小
    pub file_size: u64,
    // 文件标号
    pub number: u64,
    // 最大InternalKey
    pub smallest: InternalKey,
    // 最小InternalKey
    pub largest: InternalKey,
}

impl FileMetaData {
    ///根据大小计算文件的查找次数
    #[inline]
    pub fn init_allowed_seeks(&self) {
        // 见accumulate函数
        // 一次seek花费的时间大约相当于compact 40KB的数据，基于保守的角度考虑 每16KB的数据 等价于一次seek
        let mut allowed_seeks = self.file_size as usize / (16 * 1024);
        // 最小的查询次数100
        if allowed_seeks < 100 {
            allowed_seeks = 100 // the min seeks allowed
        }
        self.allowed_seeks.store(allowed_seeks, Ordering::Release);
    }
}

impl PartialEq for FileMetaData {
    fn eq(&self, other: &FileMetaData) -> bool {
        self.file_size == other.file_size
            && self.number == other.number
            && self.smallest == other.smallest
            && self.largest == other.largest
    }
}
impl Eq for FileMetaData {}

impl Default for FileMetaData {
    fn default() -> Self {
        FileMetaData {
            allowed_seeks: AtomicUsize::new(0),
            file_size: 0,
            number: 0,
            smallest: InternalKey::default(),
            largest: InternalKey::default(),
        }
    }
}

/// 版本之间文件变化的信息
#[derive(Default, Debug)]
pub struct FileDelta {
    // 每个compaction_pointers包含(level,InternalKey)  用来跟踪每个层级（level）压缩状态的指针从那个internalkey开始
    pub compaction_pointers: Vec<(usize, InternalKey)>,
    // 被删除文件集(level, file_number)做过滤用
    pub deleted_files: HashSet<(usize, u64)>,
    // 新添加的文件数组(level, FileMetaData)
    pub new_files: Vec<(usize, FileMetaData)>,
}

/// version更新总结
/// version（旧） + 版本编辑 = version新）
pub struct VersionEdit {
    max_levels: usize,
    // comparator name
    pub comparator_name: Option<String>,
    // file number of .log
    pub log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    // the last used sequence number
    pub last_sequence: Option<u64>,

    pub file_delta: FileDelta,
}

impl VersionEdit {
    pub fn new(max_levels: usize) -> Self {
        Self {
            max_levels,
            comparator_name: None,
            log_number: None,
            prev_log_number: None,
            next_file_number: None,
            last_sequence: None,
            file_delta: FileDelta {
                deleted_files: HashSet::default(),
                new_files: Vec::new(),
                compaction_pointers: Vec::new(),
            },
        }
    }

    /// Reset the VersionEdit to initial state except the `compaction_pointer` for
    #[inline]
    pub fn clear(&mut self) {
        self.comparator_name = None;
        self.log_number = None;
        self.prev_log_number = None;
        self.next_file_number = None;
        self.last_sequence = None;
        self.file_delta.deleted_files.clear();
        self.file_delta.new_files.clear();
        // NOTICE: compaction pointers are not cleared here
    }

    /// Add the specified file at the specified number
    pub fn add_file(
        &mut self,
        level: usize,
        file_number: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) {
        self.file_delta.new_files.push((
            level,
            FileMetaData {
                allowed_seeks: AtomicUsize::new(0),
                file_size,
                number: file_number,
                smallest,
                largest,
            },
        ))
    }

    /// Delete the specified file from the specified level
    #[inline]
    pub fn delete_file(&mut self, level: usize, file_number: u64) {
        self.file_delta.deleted_files.insert((level, file_number));
    }

    #[inline]
    pub fn set_comparator_name(&mut self, name: String) {
        self.comparator_name = Some(name);
    }

    #[inline]
    pub fn set_log_number(&mut self, log_num: u64) {
        self.log_number = Some(log_num);
    }

    #[inline]
    pub fn set_prev_log_number(&mut self, num: u64) {
        self.prev_log_number = Some(num);
    }

    #[inline]
    pub fn set_next_file(&mut self, file_num: u64) {
        self.next_file_number = Some(file_num);
    }

    #[inline]
    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    /// 将VersionEdit的信息保存到dst中
    /// 并且将其写入到manifest
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        if let Some(cmp_name) = &self.comparator_name {
            VarintU32::put_varint(dst, Comparator as u32);
            VarintU32::put_varint_prefixed_slice(dst, cmp_name.as_bytes());
        }
        if let Some(log_number) = &self.log_number {
            VarintU32::put_varint(dst, LogNumber as u32);
            VarintU64::put_varint(dst, *log_number);
        }
        if let Some(pre_ln) = &self.prev_log_number {
            VarintU32::put_varint(dst, PrevLogNumber as u32);
            VarintU64::put_varint(dst, *pre_ln);
        }
        if let Some(next_fn) = &self.next_file_number {
            VarintU32::put_varint(dst, NextFileNumber as u32);
            VarintU64::put_varint(dst, *next_fn);
        }

        if let Some(last_seq) = &self.last_sequence {
            VarintU32::put_varint(dst, LastSequence as u32);
            VarintU64::put_varint(dst, *last_seq);
        }

        for (level, key) in self.file_delta.compaction_pointers.iter() {
            VarintU32::put_varint(dst, CompactPointer as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU32::put_varint_prefixed_slice(dst, key.data());
        }

        for (level, file_num) in self.file_delta.deleted_files.iter() {
            VarintU32::put_varint(dst, DeletedFile as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU64::put_varint(dst, *file_num);
        }

        for (level, file_meta) in self.file_delta.new_files.iter() {
            VarintU32::put_varint(dst, NewFile as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU64::put_varint(dst, file_meta.number);
            VarintU64::put_varint(dst, file_meta.file_size);
            VarintU32::put_varint_prefixed_slice(dst, file_meta.smallest.data());
            VarintU32::put_varint_prefixed_slice(dst, file_meta.largest.data());
        }
    }
    // 将输入的二进制数组 src 解码并填充到调用对象的各个属性中
    pub fn decoded_from(&mut self, src: &[u8]) -> Result<()> {
        self.clear();
        let mut msg = String::new();
        let mut s = src;
        while !s.is_empty() {
            // 按标签处理数据
            if let Some(tag) = VarintU32::drain_read(&mut s) {
                match Tag::from(tag) {
                    Comparator => {
                        // 解码并存储比较器名称
                        if let Some(cmp) = VarintU32::get_varint_prefixed_slice(&mut s) {
                            match String::from_utf8(cmp.to_owned()) {
                                Ok(s) => self.comparator_name = Some(s),
                                Err(e) => return Err(Error::UTF8Error(e)),
                            }
                        } else {
                            msg.push_str("comparator name");
                            break;
                        }
                    }
                    LogNumber => {
                        // 解码并存储日志编号
                        if let Some(log_num) = VarintU64::drain_read(&mut s) {
                            self.log_number = Some(log_num);
                        } else {
                            msg.push_str("log number");
                            break;
                        }
                    }
                    NextFileNumber => {
                        // decode next file number
                        if let Some(next_file_num) = VarintU64::drain_read(&mut s) {
                            self.next_file_number = Some(next_file_num);
                        } else {
                            msg.push_str("previous log number");
                            break;
                        }
                    }
                    LastSequence => {
                        // 解码并存储最后的序列号
                        if let Some(last_seq) = VarintU64::drain_read(&mut s) {
                            self.last_sequence = Some(last_seq);
                        } else {
                            msg.push_str("last sequence number");
                            break;
                        }
                    }
                    CompactPointer => {
                        // decode compact pointer
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(key) = get_internal_key(&mut s) {
                                self.file_delta
                                    .compaction_pointers
                                    .push((level as usize, key));
                                continue;
                            }
                        }
                        msg.push_str("compaction pointer");
                        break;
                    }
                    DeletedFile => {
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(file_num) = VarintU64::drain_read(&mut s) {
                                self.file_delta
                                    .deleted_files
                                    .insert((level as usize, file_num));
                                continue;
                            }
                        }
                        msg.push_str("deleted file");
                        break;
                    }
                    NewFile => {
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(number) = VarintU64::drain_read(&mut s) {
                                if let Some(file_size) = VarintU64::drain_read(&mut s) {
                                    if let Some(smallest) = get_internal_key(&mut s) {
                                        if let Some(largest) = get_internal_key(&mut s) {
                                            self.file_delta.new_files.push((
                                                level as usize,
                                                FileMetaData {
                                                    allowed_seeks: AtomicUsize::new(0),
                                                    file_size,
                                                    number,
                                                    smallest,
                                                    largest,
                                                },
                                            ));
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        msg.push_str("new-file entry");
                        break;
                    }
                    PrevLogNumber => {
                        // decode pre log number
                        if let Some(pre_ln) = VarintU64::drain_read(&mut s) {
                            self.prev_log_number = Some(pre_ln);
                        } else {
                            msg.push_str("previous log number");
                            break;
                        }
                    }
                    Unknown => {
                        msg.push_str("unknown tag");
                        break;
                    }
                }
            } else if !src.is_empty() {
                msg.push_str("invalid tag");
            } else {
                break;
            }
        }
        if !msg.is_empty() {
            let mut m = "VersionEdit: ".to_owned();
            m.push_str(msg.as_str());
            return Err(Error::Corruption(m));
        }
        Ok(())
    }
}

impl Debug for VersionEdit {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "VersionEdit {{")?;
        if let Some(comparator) = &self.comparator_name {
            write!(f, "\n  Comparator: {}", comparator)?;
        }
        if let Some(log_number) = &self.log_number {
            write!(f, "\n  LogNumber: {}", log_number)?;
        }
        if let Some(prev_log_num) = &self.prev_log_number {
            write!(f, "\n  PrevLogNumber: {}", prev_log_num)?;
        }
        if let Some(next_file_num) = &self.next_file_number {
            write!(f, "\n  NextFile: {}", next_file_num)?;
        }
        if let Some(last_seq) = &self.last_sequence {
            write!(f, "\n  LastSeq: {}", last_seq)?;
        }
        for (level, key) in self.file_delta.compaction_pointers.iter() {
            write!(f, "\n  CompactPointer: @{} {:?}", level, key)?;
        }
        for (level, file_num) in self.file_delta.deleted_files.iter() {
            write!(f, "\n  DeleteFile: @{} #{}", level, file_num)?;
        }
        for (level, meta) in self.file_delta.new_files.iter() {
            write!(
                f,
                "\n  AddFile: @{} #{} {}bytes range: [{:?}, {:?}]",
                level, meta.number, meta.file_size, meta.smallest, meta.largest
            )?;
        }
        write!(f, "\n}}\n")?;
        Ok(())
    }
}
// 从block中读取internal_key
fn get_internal_key(mut src: &mut &[u8]) -> Option<InternalKey> {
    VarintU32::get_varint_prefixed_slice(&mut src).map(|s| InternalKey::decoded_from(s))
}
// 从block中读取level
fn get_level(max_levels: usize, src: &mut &[u8]) -> Option<u32> {
    VarintU32::drain_read(src).and_then(|l| {
        if l <= max_levels as u32 {
            Some(l)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::db::format::{InternalKey, ValueType};
    use crate::version::version_edit::VersionEdit;

    fn assert_encode_decode(edit: &VersionEdit) {
        let mut encoded = vec![];
        edit.encode_to(&mut encoded);
        let mut parsed = VersionEdit::new(7);
        parsed.decoded_from(encoded.as_slice()).expect("");
        let mut encoded2 = vec![];
        parsed.encode_to(&mut encoded2);
        assert_eq!(encoded, encoded2)
    }

    impl VersionEdit {
        fn add_compaction_pointer(&mut self, level: usize, key: InternalKey) {
            self.file_delta.compaction_pointers.push((level, key))
        }
    }

    #[test]
    fn test_encode_decode() {
        let k_big = 1u64 << 50;
        let mut edit = VersionEdit::new(7);
        for i in 0..4 {
            assert_encode_decode(&edit);
            edit.add_file(
                3,
                k_big + 300 + i,
                k_big + 400 + i,
                InternalKey::new("foo".as_bytes(), k_big + 500 + i, ValueType::Value),
                InternalKey::new("zoo".as_bytes(), k_big + 700 + i, ValueType::Deletion),
            );
            edit.delete_file(4, k_big + 700 + i);
            edit.add_compaction_pointer(
                i as usize,
                InternalKey::new("x".as_bytes(), k_big + 900 + i, ValueType::Value),
            );
        }
        edit.set_comparator_name("foo".to_owned());
        edit.set_log_number(k_big + 100);
        edit.set_next_file(k_big + 200);
        edit.set_last_sequence(k_big + 1000);
        assert_encode_decode(&edit);
    }

    #[test]
    fn test_set_comparator_name() {
        let mut edit = VersionEdit::new(7);
        let filename = String::from("Hello");
        edit.set_comparator_name(filename);
        assert_eq!("Hello", edit.comparator_name.unwrap().as_str());
    }

    #[test]
    fn test_set_log_number() {
        let mut edit = VersionEdit::new(7);
        let log_num = u64::max_value();
        edit.set_log_number(log_num);
        assert_eq!(edit.log_number.unwrap(), log_num);
    }

    #[test]
    fn test_set_prev_log_number() {
        let mut edit = VersionEdit::new(7);
        let prev_log_num = u64::max_value();
        edit.set_prev_log_number(prev_log_num);
        assert_eq!(edit.prev_log_number.unwrap(), prev_log_num);
    }

    #[test]
    fn test_set_next_file() {
        let mut edit = VersionEdit::new(7);
        let next_file = u64::max_value();
        edit.set_next_file(next_file);
        assert_eq!(edit.next_file_number.unwrap(), next_file);
    }

    #[test]
    fn test_set_last_sequence() {
        let mut edit = VersionEdit::new(7);
        let last_sequence = u64::max_value();
        edit.set_last_sequence(last_sequence);
        assert_eq!(edit.last_sequence.unwrap(), last_sequence);
    }
}
