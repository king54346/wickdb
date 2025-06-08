use std::mem::transmute;
use std::ptr::copy_nonoverlapping;

/// 32 位整数以小端字节序（least-endian）编码，并存储到一个给定的字节数组（dst）中
/// # Panics
/// Panics if `dst.len()` is less than 4.
pub fn encode_fixed_32(dst: &mut [u8], value: u32) {
    assert!(dst.len() >= 4, "the length of 'dst' must be at least 4 for a u32, but got {}", dst.len());
    dst[0..4].copy_from_slice(&value.to_le_bytes());
}

/// 64 位整数以小端字节序（least-endian）编码，并存储到一个给定的字节数组（dst）中
pub fn encode_fixed_64(dst: &mut [u8], value: u64) {
    assert!(dst.len() >= 8, "the length of 'dst' must be at least 8 for a u64, but got {}", dst.len());
    dst[0..8].copy_from_slice(&value.to_le_bytes());
}

/// 于从字节数组中读取前 4,并将它们解码为 32 位
pub fn decode_fixed_32(src: &[u8]) -> u32 {
    let mut data: u32 = 0;
    if src.len() >= 4 {
        unsafe {
            copy_nonoverlapping(src.as_ptr(), &mut data as *mut u32 as *mut u8, 4);
        }
    } else {
        for (i, b) in src.iter().enumerate() {
            data += (u32::from(*b)) << (i * 8);
        }
    }
    data.to_le()
}

/// 于从字节数组中读取前 8 ,并将它们解码为 64 位
pub fn decode_fixed_64(src: &[u8]) -> u64 {
    let mut data: u64 = 0;
    if src.len() >= 8 {
        unsafe {
            copy_nonoverlapping(src.as_ptr(), &mut data as *mut u64 as *mut u8, 8);
        }
    } else {
        for (i, b) in src.iter().enumerate() {
            data += (u64::from(*b)) << (i * 8);
        }
    }
    data.to_le()
}

///   32 位整数编码为字节序列追加到Vec中
pub fn put_fixed_32(dst: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; 4];
    encode_fixed_32(&mut buf[..], value);
    dst.extend_from_slice(&buf);
}

pub fn put_fixed_64(dst: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; 8];
    encode_fixed_64(&mut buf[..], value);
    dst.extend_from_slice(&buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_fixed_32() {
        let mut tests: Vec<(u32, Vec<u8>, Vec<u8>)> = vec![
            (0u32, vec![0; 4], vec![0, 0, 0, 0]),
            (1u32, vec![0; 4], vec![1, 0, 0, 0]),
            (255u32, vec![0; 4], vec![255, 0, 0, 0]),
            (256u32, vec![0; 4], vec![0, 1, 0, 0]),
            (512u32, vec![0; 4], vec![0, 2, 0, 0]),
            (u32::max_value(), vec![0; 4], vec![255, 255, 255, 255]),
            (u32::max_value(), vec![0; 6], vec![255, 255, 255, 255, 0, 0]),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_32(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }

    #[test]
    fn test_decode_fixed_32() {
        let mut tests: Vec<(Vec<u8>, u32)> = vec![
            (vec![], 0u32),
            (vec![0], 0u32),
            (vec![1, 0], 1u32),
            (vec![1, 1], 257u32),
            (vec![0, 0, 0, 0], 0u32),
            (vec![1, 0, 0, 0], 1u32),
            (vec![255, 0, 0, 0], 255u32),
            (vec![0, 1, 0, 0], 256u32),
            (vec![0, 1], 256u32),
            (vec![0, 2, 0, 0], 512u32),
            (vec![255, 255, 255, 255], u32::max_value()),
            (vec![255, 255, 255, 255, 0, 0], u32::max_value()),
            (vec![255, 255, 255, 255, 1, 0], u32::max_value()),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_32(src.as_slice());
            assert_eq!(result, expect);
        }
    }

    #[test]
    fn test_encode_fixed_64() {
        let mut tests: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (0u64, vec![0; 8], vec![0; 8]),
            (1u64, vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0]),
            (255u64, vec![0; 8], vec![255, 0, 0, 0, 0, 0, 0, 0]),
            (256u64, vec![0; 8], vec![0, 1, 0, 0, 0, 0, 0, 0]),
            (512u64, vec![0; 8], vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                u64::max_value(),
                vec![0; 8],
                vec![255, 255, 255, 255, 255, 255, 255, 255],
            ),
            (
                u64::max_value(),
                vec![0; 10],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
            ),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_64(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }

    #[test]
    fn test_decode_fixed_64() {
        let mut tests: Vec<(Vec<u8>, u64)> = vec![
            (vec![], 0u64),
            (vec![0], 0u64),
            (vec![0; 8], 0u64),
            (vec![1, 0], 1u64),
            (vec![1, 1], 257u64),
            (vec![1, 0, 0, 0, 0, 0, 0, 0], 1u64),
            (vec![255, 0, 0, 0, 0, 0, 0, 0], 255u64),
            (vec![0, 1, 0, 0, 0, 0, 0, 0], 256u64),
            (vec![0, 1], 256u64),
            (vec![0, 2, 0, 0, 0, 0, 0, 0], 512u64),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 1, 0],
                u64::max_value(),
            ),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_64(src.as_slice());
            assert_eq!(result, expect);
        }
    }

    #[test]
    fn test_put_fixed32() {
        let mut s: Vec<u8> = vec![];
        for i in 0..100000u32 {
            put_fixed_32(&mut s, i);
        }
        for i in 0..100000u32 {
            let res = decode_fixed_32(s.as_mut_slice());
            assert_eq!(i, res);
            s.drain(0..4);
        }
    }

    #[test]
    fn test_put_fixed64() {
        let mut s: Vec<u8> = vec![];
        for power in 0..=63u64 {
            let v = 1 << power;
            put_fixed_64(&mut s, v - 1);
            put_fixed_64(&mut s, v);
            put_fixed_64(&mut s, v + 1);
        }
        for power in 0..=63u64 {
            let v = 1 << power;
            assert_eq!(v - 1, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
            assert_eq!(v, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
            assert_eq!(v + 1, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
        }
    }
}
