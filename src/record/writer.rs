use crate::record::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::storage::File;
use crate::util::coding::encode_fixed_32;
use crate::util::crc32;
use crate::Result;

/// Writer 将记录写入底层日志“文件”
pub struct Writer<F: File> {
    // 写入数据的目标文件
    dest: F,
    // 用于表示当前块（block）中的偏移量
    block_offset: usize,
    // 缓存存储了不同记录类型的初始CRC值，为了和data一起计算新的crc
    crc_cache: [u32; (RecordType::Last as usize + 1) as usize],
}


impl<F: File> Writer<F> {
    pub fn new(dest: F) -> Self {
        let n = RecordType::Last as usize;
        let mut cache = [0; RecordType::Last as usize + 1];
        // 迭代从 1 到 n（即 1 到 RecordType::Last as usize）
        for h in 1..=n {
            // 创建一个长度为 1 的数组，包含 RecordType 中对应值的 u8 表示
            let v: [u8; 1] = [RecordType::from(h) as u8];
            // 计算该值的 CRC32 校验和并存储在 cache 数组中
            cache[h as usize] = crc32::hash(&v);
        }
        Self {
            dest,
            block_offset: 0,
            crc_cache: cache,
        }
    }

    /// 将一个字节切片追加到底层日志文件中
    pub fn add_record(&mut self, s: &[u8]) -> Result<()> {
        let mut left = s.len(); // 剩余要写入的数据长度
        let mut begin = true;   // 一开始要么first要么full

        loop {
            // 断言块偏移量没有超出块的最大大小
            assert!(
                BLOCK_SIZE >= self.block_offset,
                "[record writer] the 'block_offset' {} overflows the max BLOCK_SIZE {}",
                self.block_offset,
                BLOCK_SIZE,
            );

            let leftover = BLOCK_SIZE - self.block_offset; // 当前块中剩余的空间

            // 如果剩余空间不足以容纳记录头部，则切换到新块
            if leftover < HEADER_SIZE {
                self.fill_block_with_zeros(leftover)?;
                self.block_offset = 0; // 使用新块
            }

            let space = BLOCK_SIZE - self.block_offset - HEADER_SIZE; // 当前块中可写入数据的空间
            let to_write = left.min(space); // 计算这次要写入的数据量
            let end = to_write == left; // 判断这次写入是否为最后一块数据

            // 确定记录类型
            let record_type = match (begin, end) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            let start = s.len() - left; // 计算当前写入数据的起始位置
            self.write(record_type, &s[start..start + to_write])?; // 写入数据
            left -= to_write; // 更新剩余要写入的数据量
            begin = false;    // 标记后续记录为Middle或Last

            if left == 0 {
                break;
            }
        }

        Ok(()) // 写入完成，返回Ok
    }

    #[inline]
    fn fill_block_with_zeros(&mut self, leftover: usize) -> Result<()> {
        if leftover > 0 {
            self.dest.write(&vec![0; leftover])?;
        }
        Ok(())
    }
    /// Sync the underlying file
    #[inline]
    pub fn sync(&mut self) -> Result<()> {
        self.dest.flush()
    }

    // 将格式化的字节写入文件中 输入 rt（记录类型）和 data（字节数组)
    fn write(&mut self, rt: RecordType, data: &[u8]) -> Result<()> {
        let size = data.len();
        // 数据长度必须适合2字节
        assert!(
            size <= 0xffff,
            "[record writer] the data length in a record must fit 2 bytes but got {}",
            size
        );
        // Record加上头部大小不超过BLOCK_SIZE
        assert!(
            self.block_offset + HEADER_SIZE + size <= BLOCK_SIZE,
            "[record writer] new record [{:?}] overflows the BLOCK_SIZE [{}]",
            rt,
            BLOCK_SIZE,
        );
        // 编码头部
        let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
        buf[4] = (size & 0xff) as u8; // data length
        buf[5] = (size >> 8) as u8;
        buf[6] = rt as u8; // record type

        // 计算并编码CRC校验
        // 从缓存中获取与记录类型 rt 对应的初始CRC值
        // 将初始CRC值和新数据 data 结合起来计算包含新数据的新的CRC值。 crc32::extend 用于在已有的CRC基础上计算新的CRC值
        let mut crc = crc32::extend(self.crc_cache[rt as usize], data);
        crc = crc32::mask(crc);
        encode_fixed_32(&mut buf, crc);

        // 写入头部和数据
        self.dest.write(&buf)?;
        self.dest.write(data)?;
        // self.dest.flush()?;
        // 更新块偏移量
        self.block_offset += HEADER_SIZE + size;
        Ok(())
    }
}
