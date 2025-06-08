use crate::filter::FilterPolicy;
use crate::util::coding::{decode_fixed_32, put_fixed_32};
use std::sync::Arc;

const FILTER_BASE_LG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LG; // 2KiB
const FILTER_META_LENGTH: usize = 5; // 4bytes filter offsets length + 1bytes base log
const FILTER_OFFSET_LEN: usize = 4; // u32 length
/// A `FilterBlockBuilder` is used to construct all of the filters for a
/// particular Table.  It generates a single string which is stored as
/// a special block in the Table.
pub struct FilterBlockBuilder {
    policy: Arc<dyn FilterPolicy>,
    // 存储当前需要生成过滤器的键的集合
    keys: Vec<Vec<u8>>,
    // 存储生成的过滤器数据
    //
    // |----- filter data -----|----- filter offsets ----|--- filter offsets len ---|--- BASE_LG ---|
    //                                   num * 4 bytes              4 bytes               1 byte
    data: Vec<u8>,
    // 存储每个过滤器在 data 中的起始偏移量。
    filter_offsets: Vec<u32>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Arc<dyn FilterPolicy>) -> Self {
        Self {
            policy,
            keys: vec![],
            filter_offsets: vec![],
            data: vec![],
        }
    }

    /// 将给定的键添加到 keys 向量中
    pub fn add_key(&mut self, key: &[u8]) {
        // TODO: remove this clone
        let key = Vec::from(key);
        self.keys.push(key);
    }

    /// 根据给定的`block_offset`生成filter data
    /// `block_offset`表示当前数据块在 sstable 文件中的偏移量
    pub fn start_block(&mut self, block_offset: u64) {
        // 计算给定块偏移量的过滤器索引
        // filter_base 2kb,每2kb的key生成一个filter
        // 索引为 i 的过滤器处理偏移量范围 [i* FILTER_BASE, (i + 1) * FILTER_BASE) 中的块数据
        let filter_index = block_offset / FILTER_BASE as u64;
        // 当前已生成的过滤器数量
        let filters_len = self.filter_offsets.len() as u64;
        assert!(
            filter_index >= filters_len,
            "[filter block builder] the filter block index {} should larger than built filters {}",
            filter_index,
            filters_len,
        );
        //如果数据块（0KB - 4KB） 0 到 2047 的 filter_index 为 0，2048 到 4095 的 filter_index 为 1
        // 数据块（4KB - 8KB）： 4096 到 6143 的 filter_index 为 2，6144 到 8191 的 filter_index 为 3
        // 循环调用 generate_filter 方法以生成缺少的过滤器
        while filter_index > self.filter_offsets.len() as u64 {
            self.generate_filter();
        }
    }

    /// 附加过滤器块的尾部并以字节为单位返回过滤器块数据
    pub fn finish(&mut self) -> &[u8] {
        // 如果有剩余的键未处理，调用 generate_filter 方法生成过滤器。
        if !self.keys.is_empty() {
            // clean up the remaining keys
            self.generate_filter();
        };
        // 将每个过滤器的起始偏移量追加到 data 中
        for offset in self.filter_offsets.iter() {
            put_fixed_32(&mut self.data, *offset);
        }
        // 将偏移量的长度（4 字节）追加到 data 中
        put_fixed_32(&mut self.data, self.filter_offsets.len() as u32);
        // 将基础大小（1 字节）追加到 data 中
        self.data.push(FILTER_BASE_LG as u8);
        &self.data
    }

    // 将 keys 转换为编码的过滤器向量并追加到 data 中
    fn generate_filter(&mut self) {
        // 如果 keys 为空
        if self.keys.is_empty() {
            // 记录当前数据长度作为过滤器的起始偏移量并返回。
            self.filter_offsets.push(self.data.len() as u32);
            return;
        };
        // 如果有键，也记录当前数据长度作为过滤器的起始偏移量
        self.filter_offsets.push(self.data.len() as u32);
        // 使用当前积累的键集合生成过滤器
        let filter = self.policy.create_filter(&self.keys.iter().map(|vec| vec.as_slice()).collect());
        // 将生成的过滤器数据追加到当前的数据存储中
        self.data.extend(filter);
        // clear the keys
        self.keys.clear();
    }
}

pub struct FilterBlockReader {
    policy: Arc<dyn FilterPolicy>,
    // all filter block data without filter meta
    // | ----- filter data ----- | ----- filter offsets ----|
    //                                   num * 4 bytes
    data: Vec<u8>,
    // the amount of filter data
    num: usize,
    base_lg: usize,
}

impl FilterBlockReader {
    pub fn new(policy: Arc<dyn FilterPolicy>, mut filter_block: Vec<u8>) -> Self {
        let mut r = FilterBlockReader {
            policy,
            data: vec![],
            num: 0,
            base_lg: 0,
        };
        let n = filter_block.len();
        if n < FILTER_META_LENGTH {
            return r;
        }
        r.num = decode_fixed_32(&filter_block[n - FILTER_META_LENGTH..n - 1]) as usize;
        // invalid filter offsets length
        if r.num * FILTER_OFFSET_LEN + FILTER_META_LENGTH > n {
            return r;
        }
        r.base_lg = filter_block[n - 1] as usize;
        filter_block.truncate(n - FILTER_META_LENGTH);
        r.data = filter_block;
        r
    }

    /// Returns true if the given key is probably contained in the given `block_offset` block
    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        let i = block_offset as usize >> self.base_lg; // a >> b == a / (1 << b)
        if i < self.num {
            let (filter, offsets) = &self
                .data
                .split_at(self.data.len() - self.num * FILTER_OFFSET_LEN);
            let start =
                decode_fixed_32(&offsets[i * FILTER_OFFSET_LEN..(i + 1) * FILTER_OFFSET_LEN])
                    as usize;
            let end = {
                if i + 1 >= self.num {
                    // this is the last filter
                    filter.len()
                } else {
                    decode_fixed_32(
                        &offsets[(i + 1) * FILTER_OFFSET_LEN..(i + 2) * FILTER_OFFSET_LEN],
                    ) as usize
                }
            };
            let filter = &self.data[start..end];
            return self.policy.may_contain(filter, key);
        }
        // errors are treated as potential matches
        // so the iterator will look up the block
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterPolicy;
    use crate::util::hash::hash;

    struct TestHashFilter {}

    impl FilterPolicy for TestHashFilter {
        fn name(&self) -> &str {
            "TestHashFilter"
        }

        fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
            let h = hash(key, 1);
            let mut i = 0;
            while i + 4 <= filter.len() {
                if h == decode_fixed_32(&filter[i..i + 4]) {
                    return true;
                }
                i += 4;
            }
            false
        }

        fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8> {
            let mut f = vec![];
            for i in 0..keys.len() {
                let h = hash(keys[i], 1);
                put_fixed_32(&mut f, h);
            }
            f
        }
    }

    fn new_test_builder() -> FilterBlockBuilder {
        FilterBlockBuilder::new(Arc::new(TestHashFilter {}))
    }
    fn new_test_reader(block: Vec<u8>) -> FilterBlockReader {
        FilterBlockReader::new(Arc::new(TestHashFilter {}), block)
    }

    #[test]
    fn test_empty_builder() {
        let mut b = new_test_builder();
        let block = b.finish();
        assert_eq!(&[0, 0, 0, 0, FILTER_BASE_LG as u8], block);
        let r = new_test_reader(Vec::from(block));
        assert_eq!(r.key_may_match(0, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(10000, "foo".as_bytes()), true);
    }

    #[test]
    fn test_single_chunk() {
        let mut b = new_test_builder();
        b.start_block(100);
        b.add_key("foo".as_bytes());
        b.add_key("bar".as_bytes());
        b.add_key("box".as_bytes());
        b.start_block(200);
        b.add_key("box".as_bytes());
        b.start_block(300);
        b.add_key("hello".as_bytes());
        let block = b.finish();
        let r = new_test_reader(Vec::from(block));
        assert_eq!(r.key_may_match(100, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "bar".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "hello".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(100, "missing".as_bytes()), false);
        assert_eq!(r.key_may_match(100, "other".as_bytes()), false);
    }

    #[test]
    fn test_multiple_chunk() {
        let mut b = new_test_builder();
        // first filter
        b.start_block(0);
        b.add_key("foo".as_bytes());
        b.start_block(2000);
        b.add_key("bar".as_bytes());

        // second filter
        b.start_block(3100);
        b.add_key("box".as_bytes());

        // third filter is empty

        // last filter
        b.start_block(9000);
        b.add_key("box".as_bytes());
        b.add_key("hello".as_bytes());
        let block = b.finish();
        let r = new_test_reader(Vec::from(block));

        // check first filter
        assert_eq!(r.key_may_match(0, "foo".as_bytes()), true);
        assert_eq!(r.key_may_match(2000, "bar".as_bytes()), true);
        assert_eq!(r.key_may_match(0, "box".as_bytes()), false);
        assert_eq!(r.key_may_match(0, "hello".as_bytes()), false);
        // check second filter
        assert_eq!(r.key_may_match(3100, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(3100, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(3100, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(3100, "hello".as_bytes()), false);
        // check third filter (empty)
        assert_eq!(r.key_may_match(4100, "box".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(4100, "hello".as_bytes()), false);
        // check last filter
        assert_eq!(r.key_may_match(9000, "box".as_bytes()), true);
        assert_eq!(r.key_may_match(9000, "foo".as_bytes()), false);
        assert_eq!(r.key_may_match(9000, "bar".as_bytes()), false);
        assert_eq!(r.key_may_match(9000, "hello".as_bytes()), true);
    }
}
