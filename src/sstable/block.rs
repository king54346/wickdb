use crate::iterator::Iterator;
use crate::util::coding::{decode_fixed_32, put_fixed_32};
use crate::util::comparator::Comparator;
use crate::util::varint::VarintU32;
use crate::{Error, Result};
use std::cmp::{min, Ordering};
use std::sync::Arc;

// TODO: remove all magic number
const U32_LEN: usize = std::mem::size_of::<u32>();

/// `Block` is consist of one or more key/value entries and a block trailer.
/// Block entry shares key prefix with its preceding key until a `restart`
/// point reached. A block should contains at least one restart point.
/// First restart point are always zero.
///
/// Block Key/value entry:
///
/// ```text
///
///     +-------+---------+-----------+---------+--------------------+--------------+----------------+
///     | shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
///     +-----------------+---------------------+--------------------+--------------+----------------+
///
/// ```
///
///
#[derive(Clone, Debug)]
pub struct Block {
    data: Arc<Vec<u8>>,
    // 重启点数组在data中的偏移
    restart_offset: u32,
    //  重启点数组的长度
    restarts_len: u32,
}

impl Block {
    /// Create a `Block` instance.
    ///
    /// # Errors
    ///
    /// If the given `data` is invalid, return an error with `Status::Corruption`
    ///
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let size = data.len();
        if size >= U32_LEN {
            let max_restarts_allowed = (size - U32_LEN) / U32_LEN;
            let restarts_len = Self::restarts_len(&data);
            // make sure the size is enough for restarts
            if restarts_len as usize <= max_restarts_allowed {
                return Ok(Self {
                    data: Arc::new(data),
                    restart_offset: (size - (1 + restarts_len as usize) * U32_LEN) as u32,
                    restarts_len,
                });
            }
        };
        Err(Error::Corruption(
            "[block] read invalid block content".to_owned(),
        ))
    }

    /// 通过Iterator对象，调用者就可以遍历访问Block的存储的k/v对
    pub fn iter<C: Comparator>(&self, cmp: C) -> BlockIterator<C> {
        BlockIterator::new(
            cmp,
            self.data.clone(),
            self.restart_offset,
            self.restarts_len,
        )
    }

    // decoded the restarts length from block data
    #[inline]
    fn restarts_len(data: &[u8]) -> u32 {
        let size = data.len();
        decode_fixed_32(&data[size - U32_LEN..])
    }
}

impl Default for Block {
    fn default() -> Self {
        Self {
            data: Arc::new(vec![]),
            restart_offset: 0,
            restarts_len: 0,
        }
    }
}

/// Iterator for every entry in the block
pub struct BlockIterator<C: Comparator> {
    cmp: C,
    err: Option<Error>,

    // underlying block data
    data: Arc<Vec<u8>>,

    /*
      restarts
    */
    restarts: u32,      // restarts array starting offset
    restarts_len: u32,  // length of restarts array
    restart_index: u32, // current restart index

    // block offset of current entry start
    current: u32,

    /*
     Fields for entries in the Block
    */
    shared: u32,     // shared length
    not_shared: u32, // not shared length
    value_len: u32,  // value length
    key_offset: u32, // the offset of the current key in the block
    // Buffer for a completed key
    // The key is saperated in multiple segments in `data`.
    key: Vec<u8>,
}

impl<C: Comparator> BlockIterator<C> {
    pub fn new(cmp: C, data: Arc<Vec<u8>>, restarts: u32, restarts_len: u32) -> Self {
        Self {
            cmp,
            err: None,
            data,
            restarts,
            restarts_len,
            restart_index: 0,
            current: restarts,
            shared: 0,
            not_shared: 0,
            value_len: 0,
            key_offset: 0,
            key: vec![],
        }
    }

    // Returns the offset just pasts the end of the current entry
    #[inline]
    fn next_entry_offset(&self) -> u32 {
        self.key_offset + self.not_shared + self.value_len
    }

    #[inline]
    fn get_restart_point(&self, index: u32) -> u32 {
        decode_fixed_32(&self.data[self.restarts as usize + index as usize * 4..])
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
    }

    // Decodes a block entry from `current`
    // mark as corrupted when the current entry tail overflows the starting offset of restarts
    fn parse_block_entry(&mut self) -> bool {
        if self.current >= self.restarts {
            // Mark as invalid
            self.current = self.restarts;
            self.restart_index = self.restarts_len;
            return false;
        }
        let offset = self.current;
        let src = &self.data[offset as usize..];
        let (shared, n0) = VarintU32::common_read(src);
        let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
        let (value_len, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
        let n = (n0 + n1 + n2) as u32;
        if offset + n + not_shared + value_len > self.restarts {
            self.corruption_err();
            return false;
        }
        self.key_offset = self.current + n;
        self.shared = shared; // actually not be used
        self.not_shared = not_shared;
        self.value_len = value_len;
        let total_key_len = (shared + not_shared) as usize;
        self.key.resize(total_key_len, 0);
        // de-compress key
        let delta = &self.data[self.key_offset as usize..(self.key_offset + not_shared) as usize];
        for i in shared as usize..total_key_len {
            self.key[i] = delta[i - shared as usize]
        }

        // update restart index
        while self.restart_index + 1 < self.restarts_len
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1
        }
        true
    }

    #[inline]
    fn corruption_err(&mut self) {
        self.err = Some(Error::Corruption("bad entry in block".to_owned()));
        self.key.clear();
        self.current = self.restarts;
        self.restart_index = self.restarts_len
    }

    #[inline]
    fn valid_or_panic(&self) -> bool {
        if !self.valid() {
            panic!(
                "[block iterator] invalid the current data offset {}: overflows the restart {}",
                self.current, self.restarts
            )
        }
        true
    }
}

impl<C: Comparator> Iterator for BlockIterator<C> {
    #[inline]
    fn valid(&self) -> bool {
        self.err.is_none() && self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_block_entry();
    }

    fn seek_to_last(&mut self) {
        // seek to the last restart offset
        self.seek_to_restart_point(self.restarts_len - 1);
        // keep parsing block util the last
        // TODO: the buffered key cost a lot waste here
        while self.parse_block_entry() && self.next_entry_offset() < self.restarts {
            self.current = self.next_entry_offset()
        }
    }

    // find the first entry in block with key>= target
    fn seek(&mut self, target: &[u8]) {
        // binary search in restart array to find the last restart point with a key < target
        let mut left = 0;
        let mut right = self.restarts_len - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
            let src = &self.data[region_offset as usize..];
            let (shared, n0) = VarintU32::common_read(src);
            let (not_shared, n1) = VarintU32::common_read(&src[n0 as usize..]);
            let (_, n2) = VarintU32::common_read(&src[(n1 + n0) as usize..]);
            if shared != 0 {
                // The first key from restart offset should be completely stored.
                self.corruption_err();
                return;
            }
            let key_offset = region_offset + (n0 + n1 + n2) as u32;
            let key_len = (shared + not_shared) as usize;
            let mid_key = &self.data[key_offset as usize..key_offset as usize + key_len];
            match self.cmp.compare(mid_key, target) {
                Ordering::Less => left = mid,
                _ => right = mid - 1,
            }
        }

        // linear search (with restart block) for first key >= target
        // if all the keys > target, we seek to the start
        // if all the keys < target, we seek to the last
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_block_entry() {
                return;
            }
            match self.cmp.compare(self.key.as_slice(), target) {
                Ordering::Less => {}
                _ => return,
            }
            self.current = self.next_entry_offset();
        }
    }

    fn next(&mut self) {
        self.valid_or_panic();
        self.current = self.next_entry_offset();
        self.parse_block_entry();
    }

    // seek to prev restart offset and scan backwards to a restart point before current
    fn prev(&mut self) {
        let original = self.current;
        // Find the first restart point that just less than the current offset
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entries
                // marked as invalid
                self.current = self.restarts;
                self.restart_index = self.restarts_len;
                return;
            }
            self.restart_index -= 1
        }
        self.seek_to_restart_point(self.restart_index);
        // Loop until end of current entry hits the start of original entry
        while self.parse_block_entry() && self.next_entry_offset() < original {
            self.current = self.next_entry_offset()
        }
    }

    fn key(&self) -> &[u8] {
        self.valid_or_panic();
        &self.key
    }

    fn value(&self) -> &[u8] {
        self.valid_or_panic();
        let val_offset = self.next_entry_offset() - self.value_len;
        &self.data[val_offset as usize..(val_offset + self.value_len) as usize]
    }

    fn status(&mut self) -> Result<()> {
        if let Some(_err) = &self.err {
            return Err(self.err.take().unwrap());
        }
        Ok(())
    }
}

/// `BlockBuilder` 用于构建block
/// block的尾部存储了所有重启点的偏移量，查找时可用于进行二分查找对于特定的键。
pub struct BlockBuilder<C: Comparator> {
    //每个重启点之间的键值对数量 options.block_restart_interval 设置，默认16
    block_restart_interval: usize,
    cmp: C,
    // block的内容
    buffer: Vec<u8>,
    // 重启点
    restarts: Vec<u32>,
    //重启后生成的entry数
    counter: usize,
    finished: bool,
    // 记录最后添加的key
    last_key: Vec<u8>,
}

impl<C: Comparator> BlockBuilder<C> {
    pub fn new(block_restart_interval: usize, cmp: C) -> Self {
        assert!(
            block_restart_interval >= 1,
            "[block builder] invalid 'block_restart_interval' {} ",
            block_restart_interval,
        );
        Self {
            block_restart_interval,
            cmp,
            buffer: vec![],
            finished: false,
            counter: 0,
            restarts: vec![0; 1], //first restart point is at offset 0
            last_key: vec![],
        }
    }

    /// 返回block的预计大小 buffer大小 +重启点数组长度 + 重启点长度(uint32)
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    ///调用该函数完成Block的构建,压入重启点信息，并返回buffer，设置结束标记finished:
    pub fn finish(&mut self) -> &[u8] {
        // 将所有重启点的偏移量写入缓冲区
        for restart in self.restarts.iter() {
            put_fixed_32(&mut self.buffer, *restart)
        }
        // 将重启点的数量写入缓冲区
        put_fixed_32(&mut self.buffer, self.restarts.len() as u32);
        // 标记 block 已完成
        self.finished = true;
        &self.buffer
    }

    /// 添加k/v，要求：Reset()之后没有调用过Finish()；
    ///
    /// # Panic
    ///  要求:
    /// * BlockBuilder 尚未finish
    /// * 插入的键值对数量没有超过重启间隔
    /// * Key > 任何已加入的key(确保键的顺序)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        //保证新加入的key > 已加入的任何一个key
        assert!(
            !self.finished,
            "[block builder] add key value to a finished BlockBuilder "
        );
        assert!(
            self.counter <= self.block_restart_interval,
            "[block builder] BlockBuilder reaches the counter limit {} ",
            self.block_restart_interval,
        );
        assert!(
            self.buffer.is_empty()
                || self.cmp.compare(key, self.last_key.as_slice()) == Ordering::Greater,
            "[block builder] inconsistent new key [{:?}] compared to last_key {:?}",
            key,
            &self.last_key
        );
        // 如果计数器counter < block_restart_interval，则使用前缀算法压缩key，否则就把key作为一个重启点，无压缩存储
        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            //前缀压缩 计算key与last_key_的公共前缀
            let min_len = min(self.last_key.len(), key.len());
            // 计算给定的 key 与上一个 key 的共享前缀长度
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1
            }
        } else {
            // 新的重启点
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }
        let non_shared = key.len() - shared;
        // 根据上面的数据格式存储k/v对，追加到buffer中，并更新block状态。
        // | --- shared --- | --- non_shared --- | --- value length --- |
        VarintU32::put_varint(&mut self.buffer, shared as u32);
        VarintU32::put_varint(&mut self.buffer, non_shared as u32);
        VarintU32::put_varint(&mut self.buffer, value.len() as u32);
        // 根据上面的数据格式存储 k/v 对，追加到 buffer 中，并更新 block 状态。
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);
        // 更新 last_key
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1
    }


    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// 重设内容，清空各个信息,通常在Finish之后调用已构建新的block,需要调用reset
    #[inline]
    pub fn reset(&mut self) {
        assert!(
            self.finished,
            "[block] Try to reset an unfinished BlockBuilder"
        );
        self.buffer.clear();
        self.finished = false;
        self.counter = 0;
        self.restarts = vec![0; 1];
        self.last_key.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::iterator::Iterator;
    use crate::sstable::block::BlockBuilder;
    use crate::sstable::block::{Block, BlockIterator};
    use crate::util::coding::{decode_fixed_32, put_fixed_32};
    use crate::util::comparator::BytewiseComparator;
    use crate::util::varint::VarintU32;
    use std::str;

    fn new_test_block() -> Vec<u8> {
        let mut samples = vec!["1", "12", "123", "abc", "abd", "acd", "bbb"];
        let mut builder = BlockBuilder::new(3, BytewiseComparator::default());
        for key in samples.drain(..) {
            builder.add(key.as_bytes(), key.as_bytes());
        }
        // restarts: [0, 18, 42]
        // entries data size: 51
        Vec::from(builder.finish())
    }

    #[test]
    fn test_corrupted_block() {
        // Invalid data size
        let res = Block::new(vec![0, 0, 0]);
        assert!(res.is_err());

        let mut data = vec![];
        let mut test_restarts = vec![0, 10, 20];
        let length = test_restarts.len() as u32;
        for restart in test_restarts.drain(..) {
            put_fixed_32(&mut data, restart);
        }
        // Append invalid length of restarts
        put_fixed_32(&mut data, length + 1);
        let res = Block::new(data);
        assert!(res.is_err());
    }

    #[test]
    fn test_new_empty_block() {
        let ucmp = BytewiseComparator::default();
        let mut builder = BlockBuilder::new(2, ucmp);
        let data = builder.finish();
        let length = data.len();
        let restarts_len = decode_fixed_32(&data[length - 4..length]);
        let restarts = &data[..length - 4];
        assert_eq!(restarts_len, 1);
        assert_eq!(restarts.len() as u32 / 4, restarts_len);
        assert_eq!(decode_fixed_32(restarts), 0);
        let block = Block::new(Vec::from(data)).unwrap();
        let iter = block.iter(ucmp);
        assert!(!iter.valid());
    }

    #[test]
    fn test_new_block_from_bytes() {
        let data = new_test_block();
        assert_eq!(Block::restarts_len(&data), 3);
        let block = Block::new(data).unwrap();
        assert_eq!(block.restart_offset, 51);
    }

    #[test]
    fn test_simple_empty_key() {
        let ucmp = BytewiseComparator::default();
        let mut builder = BlockBuilder::new(2, ucmp);
        builder.add(b"", b"test");
        let data = builder.finish();
        let block = Block::new(Vec::from(data)).unwrap();
        let mut iter = block.iter(ucmp);
        iter.seek("".as_bytes());
        assert!(iter.valid());
        let k = iter.key();
        let v = iter.value();
        assert_eq!(str::from_utf8(k).unwrap(), "");
        assert_eq!(str::from_utf8(v).unwrap(), "test");
        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    #[should_panic]
    fn test_add_inconsistent_key() {
        let mut builder = BlockBuilder::new(2, BytewiseComparator::default());
        builder.add(b"ffffff", b"");
        builder.add(b"a", b"");
    }

    #[test]
    fn test_write_entries() {
        let mut builder = BlockBuilder::new(3, BytewiseComparator::default());
        assert!(builder.last_key.is_empty());
        // Basic key
        builder.add(b"1111", b"val1");
        assert_eq!(1, builder.counter);
        assert_eq!(&builder.last_key, b"1111");
        let (shared, n1) = VarintU32::common_read(&builder.buffer);
        assert_eq!(0, shared);
        assert_eq!(1, n1);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[n1 as usize..]);
        assert_eq!(4, non_shared);
        assert_eq!(1, n2);
        let (value_len, n3) = VarintU32::common_read(&builder.buffer[(n1 + n2) as usize..]);
        assert_eq!(4, value_len);
        assert_eq!(1, n3);
        let key_len = shared + non_shared;
        let read = (n1 + n2 + n3) as usize;
        let key = &builder.buffer[read..read + non_shared as usize];
        assert_eq!(key, b"1111");
        let val_offset = read + key_len as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val1");
        assert_eq!(&builder.last_key, b"1111");

        // Shared key
        let current = val_offset + value_len as usize;
        builder.add(b"11122", b"val2");
        let (shared, n1) = VarintU32::common_read(&builder.buffer[current..]);
        assert_eq!(shared, 3);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[current + n1 as usize..]);
        assert_eq!(non_shared, 2);
        let (value_len, n3) =
            VarintU32::common_read(&builder.buffer[current + (n1 + n2) as usize..]);
        assert_eq!(value_len, 4);
        let key_offset = current + (n1 + n2 + n3) as usize;
        let key = &builder.buffer[key_offset..key_offset + non_shared as usize];
        assert_eq!(key, b"22"); // compressed
        let val_offset = key_offset + non_shared as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val2");
        assert_eq!(&builder.last_key, b"11122");

        // Again shared key
        let current = val_offset + value_len as usize;
        builder.add(b"111222", b"val33");
        let (shared, n1) = VarintU32::common_read(&builder.buffer[current..]);
        assert_eq!(shared, 5);
        let (non_shared, n2) = VarintU32::common_read(&builder.buffer[current + n1 as usize..]);
        assert_eq!(non_shared, 1);
        let (value_len, n3) =
            VarintU32::common_read(&builder.buffer[current + (n1 + n2) as usize..]);
        assert_eq!(value_len, 5);
        let key_offset = current + (n1 + n2 + n3) as usize;
        let key = &builder.buffer[key_offset..key_offset + non_shared as usize];
        assert_eq!(key, b"2"); // compressed
        let val_offset = key_offset + non_shared as usize;
        let val = &builder.buffer[val_offset..val_offset + value_len as usize];
        assert_eq!(val, b"val33");
        assert_eq!(&builder.last_key, b"111222");
    }

    #[test]
    fn test_write_restarts() {
        let samples = vec!["1", "12", "123", "abc", "abd", "acd", "bbb"];
        let tests = vec![
            (1, vec![0, 4, 9, 15, 21, 27, 33], 39),
            (2, vec![0, 8, 20, 31], 37),
            (3, vec![0, 12, 27], 33),
        ];
        for (restarts_interval, expected, buffer_size) in tests {
            let mut builder = BlockBuilder::new(restarts_interval, BytewiseComparator::default());
            for key in samples.clone() {
                builder.add(key.as_bytes(), b"");
            }
            assert_eq!(builder.buffer.len(), buffer_size);
            assert_eq!(builder.restarts, expected);
        }
    }

    #[test]
    fn test_block_iter() {
        let ucmp = BytewiseComparator::default();
        // keys ["1", "12", "123", "abc", "abd", "acd", "bbb"]
        let data = new_test_block();
        let restarts_len = Block::restarts_len(&data);
        let block = Block::new(data).unwrap();
        let mut iter =
            BlockIterator::new(ucmp, block.data.clone(), block.restart_offset, restarts_len);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(iter.current, 0);
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.next();
        assert_eq!(iter.current, 5); // shared 1 + non_shared 1 + value_len 1 + key 1 + value + 1
        assert_eq!(iter.key_offset, 8);
        assert_eq!(iter.key(), "12".as_bytes());
        assert_eq!(iter.value(), "12".as_bytes());
        iter.prev();
        assert_eq!(iter.current, 0);
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek_to_last();
        assert_eq!(iter.key(), "bbb".as_bytes());
        assert_eq!(iter.value(), "bbb".as_bytes());
        // Seek
        iter.seek("1".as_bytes());
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek("".as_bytes());
        assert_eq!(iter.key(), "1".as_bytes());
        assert_eq!(iter.value(), "1".as_bytes());
        iter.seek("abd".as_bytes());
        assert_eq!(iter.key(), "abd".as_bytes());
        assert_eq!(iter.value(), "abd".as_bytes());
        iter.seek("bbb".as_bytes());
        assert_eq!(iter.key(), "bbb".as_bytes());
        assert_eq!(iter.value(), "bbb".as_bytes());
        iter.seek("zzzzzzzzzzzzzzz".as_bytes());
        assert!(!iter.valid());
    }

    #[test]
    fn test_read_write() {
        let ucmp = BytewiseComparator::default();
        let mut builder = BlockBuilder::new(2, ucmp);
        let tests = vec![
            ("", "empty"),
            ("1111", "val1"),
            ("1112", "val2"),
            ("1113", "val3"),
            ("abc", "1"),
            ("acd", "2"),
        ];
        for (key, val) in tests.clone() {
            builder.add(key.as_bytes(), val.as_bytes());
        }
        let data = builder.finish();
        let block = Block::new(Vec::from(data)).unwrap();
        let mut iter = block.iter(ucmp);
        assert!(!iter.valid());
        iter.seek_to_first();
        for (key, val) in tests {
            assert!(iter.valid());
            assert_eq!(iter.key(), key.as_bytes());
            assert_eq!(iter.value(), val.as_bytes());
            iter.next();
        }
        assert!(!iter.valid());
    }

    #[test]
    fn test_iter_big_entry_block() {
        let c = BytewiseComparator::default();
        let entries = vec![
            ("a", "a".repeat(10000)),
            ("b", "b".repeat(100000)),
            ("c", "c".repeat(1000000)),
        ];
        let mut blocks = vec![];
        for (k, v) in entries.clone() {
            let mut builder = BlockBuilder::new(2, c);
            builder.add(k.as_bytes(), v.as_bytes());
            let data = builder.finish();
            blocks.push(Block::new(data.to_vec()).unwrap());
        }
        for (b, (k, v)) in blocks.into_iter().zip(entries) {
            let mut iter = b.iter(c);
            assert!(!iter.valid());
            iter.seek_to_first();
            assert_eq!(k.as_bytes(), iter.key());
            assert_eq!(v.as_bytes(), iter.value());
            iter.next();
            assert!(!iter.valid());
        }
    }
}
