pub const MAX_VARINT_LEN_U32: usize = 5;
pub const MAX_VARINT_LEN_U64: usize = 10;

pub struct VarintU32;
pub struct VarintU64;

// 用来编码32位无符号整数的方法，在处理需要高效编码整数（特别是频繁变化的小整数）的应用场景中提供了一种既节省空间又提高性能的解决方案
// 而Byte是固长编码,用于编码字符串，在存储大量小数值时会导致空间浪费
// VarintU32 编码过程
// 1.二进制表示 2.分组(对于每组，我们在左边添加一个继续位) 3.添加继续位
// 258(0b100000010) ---> 1000 + 0000010  ----> 00000010 + 10001000 ---> 0x88 + 0x02
// VarintU64 ：可以编码从 0 到 2^64-1 的整数。
// VarintU32：可以编码从 0 到 2^32-1 的整数。
macro_rules! impl_varint {
    ($type:ty, $uint: ty) => {
        impl $type {
            /// 使用小端序
            /// # Panic
            /// Panic 当 `dst` 长度不足时
            pub fn write(dst: &mut [u8], mut n: $uint) -> usize {
                assert!(!dst.is_empty(), "dst is empty");
                let mut i = 0;
                while n >= 0b1000_0000 && i < dst.len() - 1 {
                    dst[i] = (n as u8) | 0b1000_0000; // 设置继续位并写入dst
                    n >>= 7; //处理接下来的 7 位
                    i += 1;
                }
                if i < dst.len() {
                    dst[i] = n as u8;
                    i + 1
                } else {
                    panic!("dst not enough length");
                }
            }

            /// 从给定字节解码 uint（32 或 64）并返回该值和读取的字节数 ( > 0)。
            //  如果发生错误或溢出，则返回“None”
            pub fn read(src: &[u8]) -> Option<($uint, usize)> {
                if src.is_empty() {
                    return None;
                }
                let mut n: $uint = 0;
                let mut shift: u32 = 0;
                let max_bits = std::mem::size_of::<$uint>() * 8;
                let max_bytes = (max_bits as f32 / 7.0).ceil() as u32;
                for (i, &b) in src.iter().enumerate() {
                    // 提取有效的7位
                    let value = <$uint>::from(b & 0b0111_1111);
                    // 左位移操作
                    match value.checked_shl(shift) {
                        Some(val) => n |= val, // 还原每一位
                        None => return None, // 处理溢出情况
                    }
                    // 检查继续位
                    if (b & 0b1000_0000) == 0 {
                        return Some((n, i + 1));
                    }
                    shift += 7;
                    // 增加对变量位移量的检查，防止数据或编码出现问题时无限位移导致的错误
                    // 2^32 - 1 是 u32 的最大值，即 4294967295。（因为 2^35 > 4294967295 > 2^34）每个字节提供7位，所以至少需要5个字节
                     if shift >= 7 * max_bytes {
                        break;
                    }
                }
                // 如果全部字节都是继续位，返回 None
                None
            }

            /// 将 `n` 作为 varint 字节附加到 dst 中。返回n编码后的字节数
            pub fn put_varint(dst: &mut Vec<u8>, mut n: $uint) -> usize {
                let start_len = dst.len();  // 记录初始长度
                loop {
                    if n < 0b1000_0000 {
                        dst.push(n as u8);
                        break;
                    } else {
                        // 不能用一个字节编码
                        dst.push((n as u8) | 0b1000_0000); // 将 n 的当前最低7位与 0b1000_0000 进行或操作
                        n >>= 7;
                    }
                }

                dst.len() - start_len
            }

            ///  `src` 编码进一个向量 `dst`，
            pub fn put_varint_prefixed_slice(dst: &mut Vec<u8>, src: &[u8]) {
                if !src.is_empty() {
                    // 长度编码
                    Self::put_varint(dst, src.len() as $uint);
                    // 追加src数据
                    dst.extend_from_slice(src);
                }
            }

            /// 解码一个以 Varint 形式 包含长度编码的字节数组，并更新src去除长度编码，返回已读取的字节数组
            pub fn get_varint_prefixed_slice<'a>(src: &mut &'a [u8]) -> Option<&'a [u8]> {
                Self::read(src).and_then(|(len, n)| {
                    let read_len = len as usize + n;
                    if read_len > src.len() {
                        return None;
                    }
                    let res = &src[n..read_len];
                    *src = &src[read_len..];
                    Some(res)
                })
            }

            /// 用于从给定的字节数组 src 中解码一个 u64 类型的值，并返回该值及已读取的字节数。
            /// 如果在解码过程中发生错误，它会返回特定的错误信息。 参照 `read`
            /// 解码成功：返回 (decoded_value, bytes_read)，bytes_read > 0
            /// 缓冲区太小：返回 (0, 0)，表示没有足够的数据来解码任何值
            /// 值溢出64位：返回 (0, -bytes_read)
            pub fn common_read(src: &[u8]) -> ($uint, isize) {
                if src.is_empty() {
                    return (0, 0);
                }
                let mut n: $uint = 0;
                let mut shift: u32 = 0;
                let max_bits = std::mem::size_of::<$uint>() * 8;
                let max_bytes = (max_bits as f32 / 7.0).ceil() as u32;
                for (i, &b) in src.iter().enumerate() {
                    // 提取有效的7位
                    let value = <$uint>::from(b & 0b0111_1111);
                    // 左位移操作
                    match value.checked_shl(shift) {
                        Some(val) => n |= val, // 还原每一位
                        None => return (0, -(i as isize + 1)), // 处理溢出情况
                    }
                    if (b & 0b1000_0000) == 0 {
                        return (n , (i + 1) as isize);
                    }
                    shift += 7;
                    if shift >= 7 * max_bytes {
                        return (0, 0)
                    }
                }
                 (0, 0)
            }

            /// 从一个给定的字节切片 src 中解码一个无符号整数（uint）返回一个解码出的整数,并更新src去除已读取的字节数
            pub fn drain_read(src: &mut &[u8]) -> Option<$uint> {
                <$type>::read(src).and_then(|(v, n)| {
                    // v解码出的整数 n是读取的字节数
                    *src = &src[n..];
                    Some(v)
                })
            }
        }
    };
}

impl_varint!(VarintU32, u32);
impl_varint!(VarintU64, u64);

#[cfg(test)]
mod tests {
    use super::*;

    /*
        我们这里只使用 VarintU64 进行测试，因为 VarintU32 的实现与 VarintU64 相同
    */
    #[test]
    fn test_write_u64() {
        // (input u64 , expected bytes)
        let tests = vec![
            (0u64, vec![0]),
            (100u64, vec![0b110_0100]),
            (129u64, vec![0b1000_0001, 0b1]),
            (258u64, vec![0b1000_0010, 0b10]),
            (
                58962304u64,
                vec![0b1000_0000, 0b1110_0011, 0b1000_1110, 0b1_1100],
            ),
        ];
        for (input, results) in tests {
            let mut bytes = Vec::with_capacity(MAX_VARINT_LEN_U64);
            // allocate index
            for _ in 0..results.len() {
                bytes.push(0);
            }
            let written = VarintU64::write(&mut bytes, input);
            assert_eq!(written, results.len());
            for (i, b) in bytes.iter().enumerate() {
                assert_eq!(results[i], *b);
            }
        }
    }

    #[test]
    fn test_read_u64() {
        #[rustfmt::skip]
        let mut test_data = vec![
            0,
            0b110_0100,
            0b1000_0001, 0b1,
            0b1000_0010, 0b10,
            0b1000_0000, 0b1110_0011, 0b1000_1110, 0b1_1100,
            0b1100_1110, 0b1000_0001, 0b1011_0101, 0b1101_1001, 0b1111_0110, 0b1010_1100, 0b1100_1110, 0b1000_0001, 0b1011_0101, 0b1101_1001, 0b1111_0110, 0b1010_1100,
        ];
        let expects = vec![
            Some((0u64, 1)),
            Some((100u64, 1)),
            Some((129u64, 2)),
            Some((258u64, 2)),
            Some((58962304u64, 4)),
            None,
        ];
        let mut idx = 0;
        while !test_data.is_empty() {
            match VarintU64::read(&test_data.as_slice()) {
                Some((i, n)) => {
                    assert_eq!(Some((i, n)), expects[idx]);
                    test_data.drain(0..n);
                }
                None => {
                    assert_eq!(None, expects[idx]);
                    test_data.drain(..);
                }
            }
            idx += 1;
        }
    }

    #[test]
    fn test_put_and_get_varint() {
        let mut buf = vec![];
        let mut numbers = vec![];
        let n = 100;
        for _ in 0..n {
            let r = rand::random::<u64>();
            VarintU64::put_varint(&mut buf, r);
            numbers.push(r);
        }
        let mut start = 0;
        for i in 0..n {
            if let Some((res, n)) = VarintU64::read(&buf.as_slice()[start..]) {
                assert_eq!(numbers[i], res);
                start += n
            }
        }
    }

    #[test]
    fn test_put_and_get_prefixed_slice() {
        let mut encoded = vec![];
        let tests: Vec<Vec<u8>> = vec![vec![1], vec![1, 2, 3, 4, 5], vec![0; 100]];
        for input in tests.clone() {
            VarintU64::put_varint_prefixed_slice(&mut encoded, &input);
        }
        let mut s = encoded.as_slice();
        let mut decoded = vec![];
        while s.len() > 0 {
            match VarintU64::get_varint_prefixed_slice(&mut s) {
                Some(res) => decoded.push(res.to_owned()),
                None => break,
            }
        }
        assert_eq!(tests.len(), decoded.len());
        for (get, want) in decoded.into_iter().zip(tests.into_iter()) {
            assert_eq!(get.len(), want.len());
            for (getv, wantv) in get.iter().zip(want.iter()) {
                assert_eq!(*getv, *wantv)
            }
        }
    }
}
