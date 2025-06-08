pub mod arena;
pub mod inlineskiplist;
pub mod skiplist;

use crate::db::format::{InternalKeyComparator, LookupKey, ValueType, INTERNAL_KEY_TAIL};
use crate::iterator::Iterator;
use crate::mem::arena::OffsetArena;
use crate::mem::inlineskiplist::{InlineSkipList, InlineSkiplistIterator};
use crate::util::coding::{decode_fixed_64, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::varint::VarintU32;
use crate::{Error, Result};
use std::cmp::Ordering;

// KeyComparator 是InternalKeyComparator 的包装器。用于跳表，跳表中存的是entry
#[derive(Clone, Default)]
pub struct KeyComparator<C: Comparator> {
    icmp: InternalKeyComparator<C>,
}

impl<C: Comparator> Comparator for KeyComparator<C> {
    // a和b是entry
    fn compare(&self, mut a: &[u8], mut b: &[u8]) -> Ordering {
        // 获取InternalKey
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        if ia.is_empty() || ib.is_empty() {
            // 使用内置比较
            ia.cmp(ib)
        } else {
            self.icmp.compare(ia, ib)
        }
    }

    fn name(&self) -> &str {
        self.icmp.name()
    }

    fn separator(&self, mut a: &[u8], mut b: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut a);
        let ib = extract_varint32_encoded_slice(&mut b);
        self.icmp.separator(ia, ib)
    }

    fn successor(&self, mut key: &[u8]) -> Vec<u8> {
        let ia = extract_varint32_encoded_slice(&mut key);
        self.icmp.successor(ia)
    }
}

/// In-memory write buffer
pub struct MemTable<C: Comparator> {
    cmp: KeyComparator<C>,
    // 内存有序表
    table: InlineSkipList<KeyComparator<C>, OffsetArena>,
}

impl<C: Comparator> MemTable<C> {
    /// 创建
    pub fn new(max_mem_size: usize, icmp: InternalKeyComparator<C>) -> Self {
        let arena = OffsetArena::with_capacity(max_mem_size);
        let kcmp = KeyComparator { icmp };
        let table = InlineSkipList::new(kcmp.clone(), arena);
        Self { cmp: kcmp, table }
    }

    ///返回当前使用的估计内存大小
    #[inline]
    pub fn approximate_memory_usage(&self) -> usize {
        self.table.total_size()
    }

    /// `MemTableIterator`
    #[inline]
    pub fn iter(&self) -> MemTableIterator<C> {
        MemTableIterator::new(self.table.clone())
    }

    /// Returns current elements count in inner Skiplist
    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    ///  Type标识插入（Put）还是删除（Delete）如果类型为“Deletion”，通常value将为空。
    ///  一个entry的数据结构
    /// ```text
    ///   +=================================+
    ///   |       format of the entry       |
    ///   +=================================+
    ///   | varint32 of internal key length |
    ///   +---------------------------------+ ---------------
    ///   | user key bytes                  |
    ///   +---------------------------------+   internal key
    ///   | sequence (7)       |   type (1) |
    ///   +---------------------------------+ ---------------
    ///   | varint32 of value length        |
    ///   +---------------------------------+
    ///   | value bytes                     |
    ///   +---------------------------------+
    /// ```
    ///  添加一个entry ，seq，type，kv
    pub fn add(&self, seq_number: u64, val_type: ValueType, key: &[u8], value: &[u8]) {
        let key_size = key.len();
        let internal_key_size = key_size + INTERNAL_KEY_TAIL;
        let mut buf = vec![];
        // 添加 internal_key_size(varintu32编码 包含sequence+type）
        VarintU32::put_varint(&mut buf, internal_key_size as u32);
        // 追加key
        buf.extend_from_slice(key);
        // 写入序列号和值类型 固定8位
        put_fixed_64(
            &mut buf,
            (seq_number << INTERNAL_KEY_TAIL) | val_type as u64,
        );

        VarintU32::put_varint_prefixed_slice(&mut buf, value);
        // entry存储到表中
        self.table.put(buf);
    }

    /// 如果 memtable 包含 key 的值, returns it in `Some(Ok())`.
    /// 如果 memtable 包含 key 已删除, returns `Some(Err(Status::NotFound))` .
    /// 不包含key, return `None`
    pub fn get(&self, key: &LookupKey) -> Option<Result<Vec<u8>>> {
        let mk = key.mem_key();
        let mut iter = InlineSkiplistIterator::new(self.table.clone());
        iter.seek(mk);
        if iter.valid() {
            let mut e = iter.key();
            let ikey = extract_varint32_encoded_slice(&mut e);
            let key_size = ikey.len();
            // 只比较user key
            match self
                .cmp
                .icmp
                .user_comparator
                .compare(&ikey[..key_size - INTERNAL_KEY_TAIL], key.user_key())
            {
                Ordering::Equal => {
                    let tag = decode_fixed_64(&ikey[key_size - INTERNAL_KEY_TAIL..]);
                    match ValueType::from(tag & 0xff_u64) {
                        ValueType::Value => {
                            return Some(Ok(extract_varint32_encoded_slice(&mut e).to_vec()))
                        }
                        ValueType::Deletion => return Some(Err(Error::NotFound(None))),
                        ValueType::Unknown => { /* fallback to None*/ }
                    }
                }
                _ => return None,
            }
        }
        None
    }
}

// 迭代器
pub struct MemTableIterator<C: Comparator> {
    iter: InlineSkiplistIterator<KeyComparator<C>, OffsetArena>,
    // 调用 `seek` 时将 `InternalKey` 编码为 `LookupKey` 的临时缓冲区
    tmp: Vec<u8>,
}

impl<C: Comparator> MemTableIterator<C> {
    pub fn new(table: InlineSkipList<KeyComparator<C>, OffsetArena>) -> Self {
        let iter = InlineSkiplistIterator::new(table);
        Self { iter, tmp: vec![] }
    }
}

impl<C: Comparator> Iterator for MemTableIterator<C> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    // target should be an encoded `LookupKey`
    fn seek(&mut self, target: &[u8]) {
        self.tmp.clear();
        VarintU32::put_varint_prefixed_slice(&mut self.tmp, target);
        self.iter.seek(&self.tmp)
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    // Returns the internal key
    fn key(&self) -> &[u8] {
        let mut key = self.iter.key();
        extract_varint32_encoded_slice(&mut key)
    }

    // Returns the Slice represents the value
    fn value(&self) -> &[u8] {
        let mut key = self.iter.key();
        extract_varint32_encoded_slice(&mut key);
        extract_varint32_encoded_slice(&mut key)
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

// src中读取长度编码，在长度编码后面读取字节序并返回
fn extract_varint32_encoded_slice<'a>(src: &mut &'a [u8]) -> &'a [u8] {
    if src.is_empty() {
        return src;
    }
    VarintU32::get_varint_prefixed_slice(src).unwrap_or(src)
}

#[cfg(test)]
mod tests {
    use crate::db::format::{InternalKeyComparator, LookupKey, ParsedInternalKey, ValueType};
    use crate::iterator::Iterator;
    use crate::mem::MemTable;
    use crate::util::comparator::BytewiseComparator;
    use std::str;

    fn new_mem_table() -> MemTable<BytewiseComparator> {
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        MemTable::new(1 << 32, icmp)
    }

    fn add_test_data_set(memtable: &MemTable<BytewiseComparator>) -> Vec<(&str, &str)> {
        let tests = vec![
            (2, ValueType::Value, "boo", "boo"),
            (4, ValueType::Value, "foo", "val3"),
            (3, ValueType::Deletion, "foo", ""),
            (2, ValueType::Value, "foo", "val2"),
            (1, ValueType::Value, "foo", "val1"),
        ];
        let mut results = vec![];
        for (seq, t, key, value) in tests.clone().drain(..) {
            memtable.add(seq, t, key.as_bytes(), value.as_bytes());
            results.push((key, value));
        }
        results
    }

    #[test]
    fn test_memtable_add_get() {
        let memtable = new_mem_table();
        memtable.add(1, ValueType::Value, b"foo", b"val1");
        memtable.add(2, ValueType::Value, b"foo", b"val2");
        memtable.add(3, ValueType::Deletion, b"foo", b"");
        memtable.add(4, ValueType::Value, b"foo", b"val3");
        memtable.add(2, ValueType::Value, b"boo", b"boo");

        let v = memtable.get(&LookupKey::new(b"null", 10));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 10));
        assert_eq!(b"val3", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 0));
        assert!(v.is_none());
        let v = memtable.get(&LookupKey::new(b"foo", 1));
        assert_eq!(b"val1", v.unwrap().unwrap().as_slice());
        let v = memtable.get(&LookupKey::new(b"foo", 3));
        assert!(v.unwrap().is_err());
        let v = memtable.get(&LookupKey::new(b"boo", 3));
        assert_eq!(b"boo", v.unwrap().unwrap().as_slice());
    }

    #[test]
    fn test_memtable_iter() {
        let memtable = new_mem_table();
        let mut iter = memtable.iter();
        assert!(!iter.valid());
        let entries = add_test_data_set(&memtable);
        // Forward scan
        iter.seek_to_first();
        assert!(iter.valid());
        for (key, value) in entries.iter() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.next();
        }
        assert!(!iter.valid());

        // Backward scan
        iter.seek_to_last();
        assert!(iter.valid());
        for (key, value) in entries.iter().rev() {
            let k = iter.key();
            let pkey = ParsedInternalKey::decode_from(k).unwrap();
            assert_eq!(
                pkey.as_str(),
                *key,
                "expected key: {:?}, but got {:?}",
                *key,
                pkey.as_str()
            );
            assert_eq!(
                str::from_utf8(iter.value()).unwrap(),
                *value,
                "expected value: {:?}, but got {:?}",
                *value,
                str::from_utf8(iter.value()).unwrap()
            );
            iter.prev();
        }
        assert!(!iter.valid());
    }
}
