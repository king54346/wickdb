// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use crate::filter::FilterPolicy;
use crate::util::coding::{decode_fixed_64, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::varint::VarintU32;
use std::cmp::Ordering;
use std::fmt::{Debug, Error, Formatter};
use std::str;
use std::sync::Arc;

/// The max key sequence number. The value is 2^56 - 1 because the seq number
/// only takes 56 bits when is serialized to `InternalKey`
pub const MAX_KEY_SEQUENCE: u64 = (1u64 << 56) - 1;

/// The tail bytes length of an internal key
/// 7bytes sequence number + 1byte type number
pub const INTERNAL_KEY_TAIL: usize = 8;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueType {
    /// A value indicates that the key is deleted
    Deletion = 0,
    /// A normal value
    Value = 1,

    /// Unknown type
    Unknown,
}

/// `FOR_SEEK` defines the `ValueType` that should be passed when
/// constructing a `ParsedInternalKey` for seeking to a particular
/// sequence number (since we sort sequence numbers in decreasing order
/// and the value type is embedded as the low 8 bits in the sequence
/// number in internal keys, we need to use the highest-numbered
/// ValueType, not the lowest).
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;

impl From<u64> for ValueType {
    fn from(v: u64) -> Self {
        match v {
            1 => ValueType::Value,
            0 => ValueType::Deletion,
            _ => ValueType::Unknown,
        }
    }
}

/// `ParsedInternalKey` represents a internal key used in wickdb.
/// A `ParsedInternalKey` can be encoded into a `InternalKey` by `encode()`.
pub struct ParsedInternalKey<'a> {
    /// The user's normal used key
    pub user_key: &'a [u8],
    /// The sequence number of the Key
    pub seq: u64,
    /// The value type
    pub value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    /// Try to extract a `ParsedInternalKey` from given bytes.
    /// Returns `None` if data length is less than 8 or getting an unknown value type.
    pub fn decode_from(internal_key: &'a [u8]) -> Option<ParsedInternalKey<'_>> {
        let size = internal_key.len();
        if size < INTERNAL_KEY_TAIL {
            return None;
        }
        let num = decode_fixed_64(&internal_key[size - INTERNAL_KEY_TAIL..]);
        let t = ValueType::from(num & 0xff);
        if t == ValueType::Unknown {
            return None;
        }
        let seq = num >> INTERNAL_KEY_TAIL;
        Some(Self {
            user_key: &internal_key[..size - INTERNAL_KEY_TAIL],
            seq,
            value_type: t,
        })
    }

    pub fn new(key: &'a [u8], seq: u64, v_type: ValueType) -> ParsedInternalKey<'_> {
        ParsedInternalKey {
            user_key: key,
            seq,
            value_type: v_type,
        }
    }

    /// Return the inner user key as a &str
    pub fn as_str(&self) -> &'a str {
        str::from_utf8(self.user_key).unwrap()
    }

    /// Returns a `InternalKey` encoded from the `ParsedInternalKey` using
    /// the format described in the below comment of `InternalKey`
    #[inline]
    pub fn encode(&self) -> InternalKey {
        InternalKey::new(self.user_key, self.seq, self.value_type)
    }
}

impl<'a> Debug for ParsedInternalKey<'a> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "{:?} @ {} : {:?}",
            self.user_key, self.seq, self.value_type
        )
    }
}

/// A `InternalKey` is a encoding of a `ParsedInternalKey`
///
/// The format of `InternalKey`:
///
/// ```text
/// | ----------- n bytes ----------- | --- 7 bytes --- | - 1 byte - |
///              user key                  seq number        type
/// ```
///
// TODO: use &'a [u8] instead of Vec<u8>
#[derive(Default, Clone, PartialEq, Eq)]
pub struct InternalKey {
    data: Vec<u8>,
}

impl InternalKey {
    pub fn new(key: &[u8], seq: u64, t: ValueType) -> Self {
        let mut v = Vec::from(key);
        put_fixed_64(&mut v, pack_seq_and_type(seq, t));
        InternalKey { data: v }
    }

    #[inline]
    pub fn decoded_from(src: &[u8]) -> Self {
        // TODO: avoid copy here
        Self {
            data: Vec::from(src),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        let length = self.data.len();
        &self.data[..length - INTERNAL_KEY_TAIL]
    }

    /// Returns a `ParsedInternalKey`
    pub fn parsed(&self) -> Option<ParsedInternalKey<'_>> {
        let size = self.data.len();
        let user_key = &(self.data.as_slice())[..size - INTERNAL_KEY_TAIL];
        let num = decode_fixed_64(&(self.data.as_slice())[size - INTERNAL_KEY_TAIL..]);
        let t = ValueType::from(num & 0xff_u64);
        match t {
            ValueType::Unknown => None,
            _ => Some(ParsedInternalKey {
                user_key,
                seq: num >> 8,
                value_type: t,
            }),
        }
    }
}

impl Debug for InternalKey {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        if let Some(parsed) = self.parsed() {
            write!(f, "{:?}", parsed)
        } else {
            let s = unsafe { ::std::str::from_utf8_unchecked(self.data.as_slice()) };
            write!(f, "(bad){}", s)
        }
    }
}

/// A `LookupKey` represents a 'Get' request from the user by the give key with a
/// specific sequence number to perform a MVCC style query.
/// 
/// LookupKey的格式是interna lkey length + internal key
///
/// ```text
///
///   +---------------------------------+
///   | varint32 of internal key length |
///   +---------------------------------+ --------------- user key start
///   | user key bytes                  |
///   +---------------------------------+   internal key
///   | sequence (7)        |  seek (1) |
///   +---------------------------------+ ---------------
///
/// ```
pub struct LookupKey {
    data: Vec<u8>,
    ukey_start: usize,
}

impl LookupKey {
    pub fn new(user_key: &[u8], seq_number: u64) -> Self {
        let mut data = vec![];
        let ukey_start =
            VarintU32::put_varint(&mut data, (user_key.len() + INTERNAL_KEY_TAIL) as u32);
        data.extend_from_slice(user_key);
        put_fixed_64(
            &mut data,
            pack_seq_and_type(seq_number, VALUE_TYPE_FOR_SEEK),
        );
        Self { data, ukey_start }
    }

    /// Returns a key suitable for lookup in a MemTable.
    pub fn mem_key(&self) -> &[u8] {
        &self.data
    }

    /// Returns an internal key (suitable for passing to an internal iterator)
    pub fn internal_key(&self) -> &[u8] {
        &self.data[self.ukey_start..]
    }

    /// Returns the user key
    pub fn user_key(&self) -> &[u8] {
        let len = self.data.len();
        &self.data[self.ukey_start..len - INTERNAL_KEY_TAIL]
    }
}

/// `InternalKeyComparator` 用于比较 LevelDB 的 internal key(user key+ sequence number+type tag),里面封装了user key 比较器，
/// 比较结果按以下顺序排序：
//      user key升序（根据用户提供的比较器）
//      sequence number降序
//      type降序（尽管sequence#应该足以消除歧义）
#[derive(Clone, Default)]
pub struct InternalKeyComparator<C: Comparator> {
    /// user key 比较器是`Options`的,可以自定义
    pub user_comparator: C,
}

impl<C: Comparator> InternalKeyComparator<C> {
    pub fn new(ucmp: C) -> Self {
        InternalKeyComparator {
            user_comparator: ucmp,
        }
    }
}


impl<C: Comparator> Comparator for InternalKeyComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let ua = extract_user_key(a);
        let ub = extract_user_key(b);
        // 使用用户比较器比较这两个用户键
        #[allow(clippy::comparison_chain)]
        match self.user_comparator.compare(ua, ub) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let sa = extract_seq_number(a);
                let sb = extract_seq_number(b);
                // 如果用户键相等，然后比较序列号
                // 序列号越高越新，优先处理，返回less排在最前面
                if sa > sb {
                    Ordering::Less
                } else if sa == sb {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
        }
    }

    #[inline]
    fn name(&self) -> &str {
        "leveldb.InternalKeyComparator"
    }

    fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        let start = extract_user_key(a);
        let end = extract_user_key(b);
        let mut s = self.user_comparator.separator(start, end);
        if s.len() < start.len() && self.user_comparator.compare(start, &s) == Ordering::Less {
            // Only a shorter separator is valid. Otherwise we just use `a`
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key
            put_fixed_64(
                &mut s,
                pack_seq_and_type(MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK),
            );
            s
        } else {
            a.to_owned()
        }
    }

    fn successor(&self, s: &[u8]) -> Vec<u8> {
        let ukey = extract_user_key(s);
        let mut suc = self.user_comparator.successor(ukey);
        if suc.len() < ukey.len() && self.user_comparator.compare(ukey, &suc) == Ordering::Less {
            put_fixed_64(
                &mut suc,
                pack_seq_and_type(MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK),
            );
            suc
        } else {
            s.to_owned()
        }
    }
}

/// internal key过滤策略的包装器
/// 快速判断一个键是否存在于一个集合中
pub struct InternalFilterPolicy {
    user_policy: Arc<dyn FilterPolicy>,
}

impl InternalFilterPolicy {
    pub fn new(user_policy: Arc<dyn FilterPolicy>) -> Self {
        Self { user_policy }
    }
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &str {
        self.user_policy.name()
    }

    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
        let user_key = extract_user_key(key);
        self.user_policy.may_contain(filter, user_key)
    }


    fn create_filter(&self, keys: &Vec<&[u8]>) -> Vec<u8> {
        let user_keys = keys.iter()
            .map(|key| extract_user_key(key))
            .collect();
        self.user_policy.create_filter(&user_keys)
    }
}

/// 从internal key中返回user key
#[inline]
pub fn extract_user_key(key: &[u8]) -> &[u8] {
    let size = key.len();
    assert!(
        size >= INTERNAL_KEY_TAIL,
        "[internal key] invalid size of internal key : expect >= {} but got {}",
        INTERNAL_KEY_TAIL,
        size
    );
    &key[..size - INTERNAL_KEY_TAIL]
}

// get the sequence number from a internal key slice
#[inline]
fn extract_seq_number(key: &[u8]) -> u64 {
    let size = key.len();
    assert!(
        size >= INTERNAL_KEY_TAIL,
        "[internal key] invalid size of internal key : expect >= 8 but got {}",
        size
    );
    decode_fixed_64(&key[size - INTERNAL_KEY_TAIL..]) >> INTERNAL_KEY_TAIL
}

#[inline]
// compose sequence number and value type into a single u64
fn pack_seq_and_type(seq: u64, v_type: ValueType) -> u64 {
    assert!(
        seq <= MAX_KEY_SEQUENCE,
        "[key seq] the sequence number should be <= {}, but got {}",
        MAX_KEY_SEQUENCE,
        seq
    );
    seq << 8 | v_type as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::comparator::BytewiseComparator;

    #[test]
    fn test_pack_seq_and_type() {
        let mut tests: Vec<(u64, ValueType, Vec<u8>)> = vec![
            (1, ValueType::Value, vec![1, 1, 0, 0, 0, 0, 0, 0]),
            (2, ValueType::Deletion, vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                MAX_KEY_SEQUENCE,
                ValueType::Deletion,
                vec![0, 255, 255, 255, 255, 255, 255, 255],
            ),
        ];
        for (seq, t, expect) in tests.drain(..) {
            let u = decode_fixed_64(expect.as_slice());
            assert_eq!(pack_seq_and_type(seq, t), u);
        }
    }

    #[test]
    #[should_panic]
    fn test_pack_seq_and_type_panic() {
        pack_seq_and_type(1 << 56, ValueType::Value);
    }

    fn assert_encoded_decoded(key: &str, seq: u64, vt: ValueType) {
        let encoded = InternalKey::new(key.as_bytes(), seq, vt);
        assert_eq!(key.as_bytes(), encoded.user_key());
        let decoded = encoded.parsed().expect("");
        assert_eq!(key, decoded.as_str());
        assert_eq!(seq, decoded.seq);
        assert_eq!(vt, decoded.value_type);
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let test_keys = ["", "k", "hello", "longggggggggggggggggggggg"];
        let test_seqs = [
            1,
            2,
            3,
            (1u64 << 8) - 1,
            1u64 << 8,
            (1u64 << 8) + 1,
            (1u64 << 16) - 1,
            1u64 << 16,
            (1u64 << 16) + 1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
        ];
        for i in 0..test_keys.len() {
            for j in 0..test_seqs.len() {
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::Value);
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::Deletion);
            }
        }
    }

    #[test]
    fn test_icmp_cmp() {
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        let tests = vec![
            (
                ("", 100, ValueType::Value),
                ("", 100, ValueType::Value),
                Ordering::Equal,
            ),
            (
                ("", 90, ValueType::Value),
                ("", 100, ValueType::Value),
                Ordering::Greater,
            ), // physically less but logically larger
            (
                ("", 90, ValueType::Value),
                ("", 90, ValueType::Deletion),
                Ordering::Equal,
            ), // Only cmp value seq if the user keys are same
            (
                ("a", 90, ValueType::Value),
                ("b", 100, ValueType::Value),
                Ordering::Less,
            ),
        ];
        for (a, b, expected) in tests {
            let ka = InternalKey::new(a.0.as_bytes(), a.1, a.2);
            let kb = InternalKey::new(b.0.as_bytes(), b.1, b.2);
            assert_eq!(expected, icmp.compare(ka.data(), kb.data()));
        }
    }

    #[test]
    fn test_icmp_separator() {
        let tests = vec![
            // ukey are the same
            (
                ("foo", 100, ValueType::Value),
                ("foo", 99, ValueType::Value),
                ("foo", 100, ValueType::Value),
            ),
            (
                ("foo", 100, ValueType::Value),
                ("foo", 101, ValueType::Value),
                ("foo", 100, ValueType::Value),
            ),
            (
                ("foo", 100, ValueType::Value),
                ("foo", 100, ValueType::Value),
                ("foo", 100, ValueType::Value),
            ),
            // ukey are disordered
            (
                ("foo", 100, ValueType::Value),
                ("bar", 99, ValueType::Value),
                ("foo", 100, ValueType::Value),
            ),
            // ukey are different but correctly ordered
            (
                ("foo", 100, ValueType::Value),
                ("hello", 200, ValueType::Value),
                ("g", MAX_KEY_SEQUENCE, VALUE_TYPE_FOR_SEEK),
            ),
            // When a's ukey is the prefix of b's
            (
                ("foo", 100, ValueType::Value),
                ("foobar", 200, ValueType::Value),
                ("foo", 100, ValueType::Value),
            ),
            // When b's ukey is the prefix of a's
            (
                ("foobar", 100, ValueType::Value),
                ("foo", 200, ValueType::Value),
                ("foobar", 100, ValueType::Value),
            ),
        ];
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        for (a, b, expected) in tests {
            let ka = InternalKey::new(a.0.as_bytes(), a.1, a.2);
            let kb = InternalKey::new(b.0.as_bytes(), b.1, b.2);
            assert_eq!(
                InternalKey::new(expected.0.as_bytes(), expected.1, expected.2).data(),
                icmp.separator(ka.data(), kb.data()).as_slice()
            );
        }
    }

    #[test]
    fn test_icmp_successor() {
        let tests = vec![
            (
                (Vec::from("foo".as_bytes()), 100, ValueType::Value),
                (
                    Vec::from("g".as_bytes()),
                    MAX_KEY_SEQUENCE,
                    VALUE_TYPE_FOR_SEEK,
                ),
            ),
            (
                (vec![255u8, 255u8], 100, ValueType::Value),
                (vec![255u8, 255u8], 100, ValueType::Value),
            ),
        ];
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        for (k, expected) in tests {
            assert_eq!(
                icmp.successor(InternalKey::new(k.0.as_slice(), k.1, k.2).data()),
                InternalKey::new(expected.0.as_slice(), expected.1, expected.2).data()
            );
        }
    }
}
