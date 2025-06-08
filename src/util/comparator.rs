use std::cmp::{min, Ordering};

/// Comparator 对象提供了“Slice”之间的总顺序，
// 通常用在如 SSTables或数据库
/// 比较器实现必须是线程安全的，需要多个线程同时调用
pub trait Comparator: Send + Sync + Clone + Default {
    // 如果 a 小于 b，返回 Ordering::Less。
    // 如果 a 等于 b，返回 Ordering::Equal。
    // 如果 a 大于 b，返回 Ordering::Greater
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// 返回比较器的名称，这个名称在数据库中具有特定的意义。
    /// 如果更改了比较器的实现方式，应更换比较器的名称，这是为了防止数据库在使用不同的比较逻辑时产生数据一致性问题。
    /// 例如，如果你使用一个比较器创建了数据库，然后更换了一个具有不同排序逻辑的比较器来访问相同的数据库，可能会导致错误或数据损坏。
    fn name(&self) -> &str;

    ///这个方法用于找到两个键 a 和 b 之间的一个分隔键 k，满足：
    ///
    /// compare(a, k) <= 0
    /// compare(k, b) < 0
    /// 这种方法在创建 SSTable 的索引块时非常有用，因为合理的分隔键可以减少存储需求并优化查询性能。
    /// 实现时应尽量使 k 短于 a 和 b，以减小存储开销。
    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8>;

    /// 给定键 s，此方法返回一个键 k，使得 compare(k, s) >= 0。如果 s 已经是最大可能的键（由u8组成）
    /// 则返回 s 本身。这个方法用于确定给定键的"后继"键，即在排序顺序中紧随其后的键。
    ///
    /// 这些方法加在一起，为数据结构如 SSTables 或数据库提供了一个全面的键管理框架，能够有效地支持插入、删除、查找和索引操作。
    /// 实现这个 Comparator trait 需要考虑线程安全，因为可能会从多个线程并发调用其方法，所以它包含了 Send 和 Sync trait，确保可以安全地在多线程环境中使用。
    fn successor(&self, key: &[u8]) -> Vec<u8>;
}

#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    #[inline]
    fn name(&self) -> &str {
        "leveldb.BytewiseComparator"
    }

    //中两个元素之间的可能的最小键
    #[inline]
    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        // 确定较小长度
        let min_size = std::cmp::min(a.len(), b.len());
        let mut diff_index = 0;
        // 找到第一个不同的字节
        while diff_index < min_size && a[diff_index] == b[diff_index] {
            diff_index += 1;
        }
        // 如果一个数组是另一个数组的前缀，或在最小长度内完全相同，直接返回 a 的副本。
        if diff_index >= min_size {
            return a.to_owned();
        }
        // 检查是否可以创建有效的分隔键
        if a[diff_index] != 0xff && a[diff_index] + 1 < b[diff_index] {
            let mut res = a[0..=diff_index].to_vec();  // 直接复制必要的部分到新的向量
            *res.last_mut().unwrap() += 1;              //在 a[diff_index] 处加 1 来得到一个合适的分隔键，此键将大于 a 且小于 b。
            return res;
        }

        a.to_owned()  // 在其他情况下，返回 a 的副本
    }
    // 生成一个在字节序上比输入键大的最短键
    #[inline]
    fn successor(&self, key: &[u8]) -> Vec<u8> {
        // 寻找第一个不是 \xff 的字节(\xff 是最大的字节值)
        for i in 0..key.len() {
            let byte = key[i];
            if byte != 0xff {
                let mut res: Vec<u8> = vec![0; i + 1];
                res[0..=i].copy_from_slice(&key[0..=i]);
                //  增加最后一个字节的值
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        key.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator_separator() {
        let mut tests = vec![
            ("", "1111", ""),
            ("1111", "", "1111"),
            ("1111", "111", "1111"),
            ("123", "1234", "123"),
            ("1234", "1234", "1234"),
            ("1", "2", "1"),
            ("1357", "2", "1357"),
            ("1111", "12345", "1111"),
            ("1111", "13345", "12"),
        ];
        let c = BytewiseComparator::default();
        for (a, b, expect) in tests.drain(..) {
            let res = c.separator(a.as_bytes(), b.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let a: Vec<u8> = vec![48, 255];
        let b: Vec<u8> = vec![48, 49, 50, 51];
        let res = c.separator(a.as_slice(), b.as_slice());
        assert_eq!(res, a);
    }

    #[test]
    fn test_bytewise_comparator_successor() {
        let mut tests = vec![("", ""), ("111", "2"), ("222", "3")];
        let c = BytewiseComparator::default();
        for (input, expect) in tests.drain(..) {
            let res = c.successor(input.as_bytes());
            assert_eq!(std::str::from_utf8(&res).unwrap(), expect);
        }
        // special 0xff case
        let mut corner_tests = vec![
            (vec![0xff, 0xff, 1], vec![255u8, 255u8, 2]),
            (vec![0xff, 0xff, 0xff], vec![255u8, 255u8, 255u8]),
        ];
        for (input, expect) in corner_tests.drain(..) {
            let res = c.successor(input.as_slice());
            assert_eq!(res, expect)
        }
    }
}
