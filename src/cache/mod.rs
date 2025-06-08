pub mod lru;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

/// `Cache`是将kv映射的接口。
/// 是线程安全的
/// 它可能会自动删除旧的条目以为新条目腾出空间。
/// 通过charge来实现lru策略
/// 例如，charge可以设置为key可变长度字符串的长度
///
/// 提供了lru的缓存实现
/// 用户可以自定义实现更复杂的（比如like scan-resistance，a custom eviction policy、variable cache sizing等）
/// 扫描抗性 防止缓存被一次性的大量扫描操作（如批量读取）所污染
/// ARC（Adaptive Replacement Cache，自适应替换缓存）
///
/// 缓存接口 Cache，包括插入、获取、删除和计算总容量的方法
pub trait Cache<K, V>: Sync + Send
    where
        K: Sync + Send,
        V: Sync + Send + Clone,
{
    /// 将键->值的映射插入到缓存中，并根据总缓存容量为其分配指定的charge。
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;

    /// 根据键获取对应的值
    fn get(&self, key: &K) -> Option<V>;

    /// 删除一个键值对.
    fn erase(&self, key: &K);

    /// 返回缓存中存储的所有元素的charge的估计值。
    fn total_charge(&self) -> usize;
}

/// ShardedLRUCache内部有16个LRUCache，查找Key时首先计算key属于哪一个分片，分片的计算方法是取32位hash值的高4位
/// 然后在相应的LRUCache中进行查找，这样就大大减少了多线程的访问锁的开销
/// 使用 PhantomData 来标记泛型类型参数 K 和 V
pub struct ShardedCache<K, V, C>
    where
        C: Cache<K, V>,
        K: Sync + Send,
        V: Sync + Send + Clone,
{
    shards: Arc<Vec<C>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}
// 分片缓存，每个分片是一个独立的缓存实例，通过哈希函数将键分配到不同的分片上，以减少并发访问的锁开销
impl<K, V, C> ShardedCache<K, V, C>
    where
        C: Cache<K, V>,
        K: Sync + Send + Hash + Eq,
        V: Sync + Send + Clone,
{
    /// Create a new `ShardedCache` with given shards
    pub fn new(shards: Vec<C>) -> Self {
        Self {
            shards: Arc::new(shards),
            _k: PhantomData,
            _v: PhantomData,
        }
    }
    // 根据键计算其哈希值，并确定其分片索引
    fn find_shard(&self, k: &K) -> usize {
        let mut s = DefaultHasher::new();
        let len = self.shards.len();
        k.hash(&mut s);
        s.finish() as usize % len
    }
}

impl<K, V, C> Cache<K, V> for ShardedCache<K, V, C>
    where
        C: Cache<K, V>,
        K: Sync + Send + Hash + Eq,
        V: Sync + Send + Clone,
{

    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        let idx = self.find_shard(&key);
        self.shards[idx].insert(key, value, charge)
    }

    fn get(&self, key: &K) -> Option<V> {
        let idx = self.find_shard(key);
        self.shards[idx].get(key)
    }

    fn erase(&self, key: &K) {
        let idx = self.find_shard(key);
        self.shards[idx].erase(key)
    }
    // 迭代每个分片的total_charge累加
    fn total_charge(&self) -> usize {
        self.shards.iter().fold(0, |acc, s| acc + s.total_charge())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lru::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn new_test_lru_shards(n: usize) -> Vec<LRUCache<String, String>> {
        (0..n).into_iter().fold(vec![], |mut acc, _| {
            acc.push(LRUCache::new(1 << 20));
            acc
        })
    }

    #[test]
    fn test_concurrent_insert() {
        let cache = Arc::new(ShardedCache::new(new_test_lru_shards(8)));
        let n = 4; // use 4 thread
        let repeated = 10;
        let mut hs = vec![];
        let kv: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(vec![]));
        let total_size = Arc::new(AtomicU64::new(0));
        for i in 0..n {
            let cache = cache.clone();
            let kv = kv.clone();
            let total_size = total_size.clone();
            let h = thread::spawn(move || {
                for x in 1..=repeated {
                    let k = i.to_string().repeat(x);
                    let v = k.clone();
                    {
                        let mut kv = kv.lock().unwrap();
                        (*kv).push((k.clone(), v.clone()));
                    }
                    total_size.fetch_add(x as u64, Ordering::SeqCst);
                    assert_eq!(cache.insert(k, v, x), None);
                }
            });
            hs.push(h);
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(
            total_size.load(Ordering::Relaxed) as usize,
            cache.total_charge()
        );
        for (k, v) in kv.lock().unwrap().clone() {
            assert_eq!(cache.get(&k), Some(v));
        }
    }
}
