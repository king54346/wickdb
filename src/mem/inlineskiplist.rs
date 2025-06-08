use std::cmp::Ordering as CmpOrdering;
use std::mem;
use std::ptr;
use std::ptr::{NonNull, null, null_mut};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use bytes::Bytes;
use rand::random;

use crate::{Iterator, Result};
use crate::Comparator;
use crate::mem::arena::Arena;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Bytes,
    height: usize,
    // 原子指针类型用于线程安全地读写指针值，MAX_HEIGHT定义了跳表的最大高度
    next_nodes: [AtomicPtr<Self>; MAX_HEIGHT],
}

impl Node {
    // height<=MAX_HEIGHT
    fn new<A: Arena>(key: Bytes, height: usize, arena: &A) -> *mut Self {
        //计算内存大小 ，静态大小减去未使用的 next_nodes 指针所占的空间
        let size =
            mem::size_of::<Self>() - (MAX_HEIGHT - height) * mem::size_of::<AtomicPtr<Self>>();
        // 所需的对齐字节数
        let align = mem::align_of::<Self>();
        // 内存分配
        let p = unsafe { arena.allocate::<Node>(size, align) };
        assert!(!p.is_null());
        //初始化节点
        unsafe {
            let node = &mut *p;
            // Bytes指向堆上的地址
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.height, height);
            ptr::write_bytes(node.next_nodes.as_mut_ptr(), 0, height);
            p
        }
    }

    #[inline]
    //获取当前节点在指定高度的下一个节点
    fn get_next(&self, height: usize) -> *mut Node {
        self.next_nodes[height].load(Ordering::SeqCst)
    }

    #[inline]
    // 设置当前节点在指定高度的下一个节点
    fn set_next(&self, height: usize, node: *mut Node) {
        self.next_nodes[height].store(node, Ordering::SeqCst);
    }

    #[inline]
    fn key(&self) -> &[u8] {
        &self.key
    }
}

struct InlineSkipListInner<A: Arena> {
    // 当前跳表的高度 1 <= height <= kMaxHeight
    height: AtomicUsize,
    // 一个非空的原始指针
    head: NonNull<Node>,
    // 预先分配一块内存
    arena: A,
    // 记录跳表管理的总内存大小
    // 在 arena 分配的内存只包括 Node 本身而不是 key 的内容，因为 key 仅占用很小的空间（Bytes 类型）。
    size: AtomicUsize,
}

// 线程移动所有权trait
unsafe impl<A: Arena + Send> Send for InlineSkipListInner<A> {}

// 线程可以同时持有并访问其引用
unsafe impl<A: Arena + Sync> Sync for InlineSkipListInner<A> {}

impl<A: Arena> Drop for InlineSkipListInner<A> {
    fn drop(&mut self) {
        // 安全地获取第一个节点的指针。
        let mut current_node = Some(self.head.as_ptr());
        while let Some(node) = current_node {
            let next_node = unsafe { (*node).get_next(0) };
            // 销毁当前节点。由于当前节点是非null的，调用 drop_in_place 是安全的。
            unsafe {
                ptr::drop_in_place(node);
            }
            // 更新当前节点为下一个节点，准备下一轮循环。
            current_node = if next_node.is_null() { None } else { Some(next_node) };
        }
    }
}

// Arc多个线程需要读取同一数据时
// Mutex 在需要跨线程修改共享数据时
#[derive(Clone)]
pub struct InlineSkipList<C: Comparator, A: Arena + Clone + Send + Sync> {
    inner: Arc<InlineSkipListInner<A>>,
    comparator: C,
}

impl<C, A> InlineSkipList<C, A> where C: Comparator, A: Arena + Clone + Send + Sync {
    pub fn new(comparator: C, arena: A) -> Self {
        // Comparator需要实现Bytes比较 utils/comparator中实现
        let head = Node::new(Bytes::new(), MAX_HEIGHT, &arena);
        // size 只包含key的大小
        Self {
            inner: Arc::new(InlineSkipListInner {
                height: AtomicUsize::new(1),
                head: unsafe { NonNull::new_unchecked(head) },
                arena,
                size: AtomicUsize::new(0),
            }),
            comparator,
        }
    }

    // findNear 查找与给定键（key）最接近的节点
    // less如果为 true，查找键小于给定键的最右侧节点；如果为 false，查找键大于给定键的最左侧节点。
    // allow_equal 指示是否允许返回键等于给定键的节点
    // 返回一个元组,如果不存在第一个为空指针,第二个元素是一个布尔值，如果找到的节点键等于给定键，则为 true。
    fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (*mut Node, bool) {
        let head = self.inner.head.as_ptr();
        let mut x = head;
        let mut level = self.get_height() - 1;
        loop {
            unsafe {
                let next_ptr = (*x).get_next(level);
                // 如果当前层级没有下一个节点，则直接向下移动到更低的层级
                if next_ptr.is_null() {
                    if level == 0 {
                        // 在最底层没有找到合适的节点，根据条件返回
                        return if !less || x == head { (null_mut(), false) } else { (x, false) };
                    }
                    level -= 1;
                    continue;
                }

                let next = &*next_ptr;
                match self.comparator.compare(key, &next.key) {
                    CmpOrdering::Greater => {
                        // 当前节点的键小于目标键，向右移动
                        x = next_ptr;
                    },
                    CmpOrdering::Equal => {
                        if allow_equal {
                            // 找到了等于目标键的节点
                            return (next_ptr, true);
                        }
                        if !less {
                            // 需要找到大于目标键的节点
                            return (next.get_next(0), false);
                        }
                        if level == 0 {
                            // 在最底层寻找小于目标键的节点
                            return if x == head { (null_mut(), false) } else { (x, false) };
                        }
                        level -= 1;
                    },
                    CmpOrdering::Less => {
                        if level == 0 {
                            // 在最底层，根据less决定是否返回当前节点
                            return if !less { (next_ptr, false) } else { if x == head { (null_mut(), false) } else { (x, false) }};
                        }
                        level -= 1;
                    },
                }
            }
        }
    }

    // 插入一个新的节点，Bytes实现了From,key的类型就可以转化为bytes使用.into()
    pub fn put(&self, key: impl Into<Bytes>) {
        let key: Bytes = key.into();
        // 节点大小更新
        self.inner.size.fetch_add(key.len(), Ordering::SeqCst);
        // 当前跳表的高度
        let mut list_height = self.get_height();
        // 存储搜索过程中每一层的前驱和后继节点指针
        let mut prev = vec![null_mut(); MAX_HEIGHT + 1];
        let mut next = vec![null_mut(); MAX_HEIGHT + 1];
        // 查找插入位置
        prev[list_height] = self.inner.head.as_ptr();
        // 遍历每一层去获取，并记录前后节点
        for i in (0..list_height).rev() {
            let (p, n) = self.find_splice_for_level(&key, prev[i + 1], i);
            prev[i] = p;
            next[i] = n;
            assert_ne!(prev[i], next[i]);
        }
        // 创建新节点
        let height = random_height();
        let np = Node::new(key, height, &self.inner.arena);
        // 更新跳表高度
        while height > list_height {
            match self.inner.height.compare_exchange_weak(
                list_height,
                height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                // 返回显示其他线程修改的实际高度
                Err(h) => list_height = h,
            }
        }
        let node = unsafe { &*np };

        // 插入节点
        // 使用循环，从底层向上插入节点到跳表中。通过原子比较并交换操作（CAS）确保节点正确插入
        for i in 0..height {
            loop {
                if prev[i].is_null() {
                    // 没有前驱不可能在最底层
                    assert!(i > 1);
                    // 因为高度超过了旧的 listHeight
                    // 从头节点开始搜索插入位置
                    let (p, n) = self.find_splice_for_level(&node.key, self.inner.head.as_ptr(), i);

                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                unsafe {
                    node.set_next(i, next[i]);
                    // 前驱节点的next的设置为当前节点
                    match &(*prev[i]).next_nodes[i].compare_exchange(
                        next[i],
                        np,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {
                            // 在同一层级 i 内搜索新的前驱和后继位置
                            let (p, n) = self.find_splice_for_level(&node.key, prev[i], i);
                            if p == n {
                                // 重新计算的前驱和后继节点相同，这种情况不应该发生
                                assert_eq!(i, 0, "Equality can happen only on base level");
                                ptr::drop_in_place(np);
                                return;
                            }
                            prev[i] = p;
                            next[i] = n;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.find_last().is_null()
    }

    pub fn len(&self) -> usize {
        let mut node = self.inner.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe { (&*node).get_next(0) };
            if !next.is_null() {
                count += 1;
                node = next;
                continue;
            }
            return count;
        }
    }

    #[inline]
    // 计算并返回跳表当前使用的总内存大小  分配器使用的内存大小(node结构大小)+ key的大小
    pub fn total_size(&self) -> usize {
        self.inner.size.load(Ordering::SeqCst) + self.inner.arena.memory_used()
    }
    // 找到最后一个节点的指针
    fn find_last(&self) -> *mut Node {
        let mut x = self.inner.head.as_ptr();
        let mut height = self.get_height() - 1;
        loop {
            unsafe {
                let next = (*x).get_next(height);
                if next.is_null() {
                    if height == 0 {
                        if x == self.inner.head.as_ptr() {
                            return null_mut();
                        }
                        return x;
                    } else {
                        height -= 1;
                    }
                } else {
                    x = next;
                }
            }
        }
    }
    // 用于在给定的层（level）上找到一个合适的插入点
    // 输入一个起始节点和高度，key， 返回前节点和后节点，插入到两节点中间
    fn find_splice_for_level(&self, key: &[u8], mut before: *mut Node, height: usize) -> (*mut Node, *mut Node) {
        loop {
            unsafe {
                // 当前节点在指定层级的下一个节点
                let next = (&*before).get_next(height);
                if next.is_null() {
                    return (before, null_mut());
                } else {
                    match self.comparator.compare(key, &(*next).key) {
                        CmpOrdering::Equal => return (next, next),
                        CmpOrdering::Less => return (before, next),
                        CmpOrdering::Greater => {
                            before = next;
                        }
                    }
                }
            }
        }
    }

    fn get_height(&self) -> usize {
        self.inner.height.load(Ordering::Relaxed)
    }
}

// 迭代器 实现迭代器 trait(自己定义)
pub struct InlineSkiplistIterator<C, A> where C: Comparator, A: Arena + Clone + Send + Sync{
    list: InlineSkipList<C, A>,
    node: *const Node,
}

impl<C, A> Iterator for InlineSkiplistIterator<C, A> where C: Comparator, A: Arena + Clone + Send + Sync{
    #[inline]
    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    fn seek_to_first(&mut self) {
        unsafe { self.node = self.list.inner.head.as_ref().get_next(0) }
    }

    fn seek_to_last(&mut self) {
        self.node = self.list.find_last();
    }

    // 找一个最接近的左侧节点
    fn seek(&mut self, key: &[u8]) {
        let (node, _) = self.list.find_near(key, false, true);
        self.node = node;
    }
    // 下一个
    fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
    }

    // 前一个
    fn prev(&mut self) {
        assert!(self.valid());
        let (node, _) = self.list.find_near(self.key(), true, false);
        self.node = node;
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.node).key() }
    }
    // 跳表中没有键值对，key代表node的value
    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}
// 创建迭代器实例
impl<C, A> InlineSkiplistIterator<C, A> where C: Comparator, A: Arena + Clone + Send + Sync{
    pub fn new(list: InlineSkipList<C, A>) -> Self {
        Self { list, node: null() }
    }
}
// 生成随机高度，但是不会超过head节点的高度
fn random_height() -> usize {
    let mut height = 1;
    //1/3的几率增加层级
    while height < MAX_HEIGHT && random::<u32>() < HEIGHT_INCREASE {
        height += 1;
    }
    height
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use crate::BytewiseComparator;
    use crate::mem::arena::OffsetArena;
    use std::thread;
    use std::time::Duration;
    use super::*;

    fn new_test_skl() -> InlineSkipList<BytewiseComparator, OffsetArena> {
        InlineSkipList::new(
            BytewiseComparator::default(),
            OffsetArena::with_capacity(1 << 20),
        )
    }

    #[test]
    fn test_mem_alloc() {
        let cmp = BytewiseComparator::default();
        let arena = OffsetArena::with_capacity(1 << 20);
        let l = InlineSkipList::new(cmp, arena);
        // Node size + algin mask
        assert_eq!(mem::size_of::<Node>() +  mem::align_of::<Node>(), l.inner.arena.memory_used());
    }

    #[test]
    fn test_find_near() {
        let cmp = BytewiseComparator::default();
        let arena = OffsetArena::with_capacity(1 << 20);
        let l = InlineSkipList::new(cmp, arena);
        for i in 0..1000 {
            let key = format!("{:05}{:08}", i * 10 + 5, 0);
            l.put(key);
        }
        let cases = vec![
            ("00001", false, false, Some("00005")),
            ("00001", false, true, Some("00005")),
            ("00001", true, false, None),
            ("00001", true, true, None),
            ("00005", false, false, Some("00015")),
            ("00005", false, true, Some("00005")),
            ("00005", true, false, None),
            ("00005", true, true, Some("00005")),
            ("05555", false, false, Some("05565")),
            ("05555", false, true, Some("05555")),
            ("05555", true, false, Some("05545")),
            ("05555", true, true, Some("05555")),
            ("05558", false, false, Some("05565")),
            ("05558", false, true, Some("05565")),
            ("05558", true, false, Some("05555")),
            ("05558", true, true, Some("05555")),
            ("09995", false, false, None),
            ("09995", false, true, Some("09995")),
            ("09995", true, false, Some("09985")),
            ("09995", true, true, Some("09995")),
            ("59995", false, false, None),
            ("59995", false, true, None),
            ("59995", true, false, Some("09995")),
            ("59995", true, true, Some("09995")),
        ];
        for (i, (key, less, allow_equal, exp)) in cases.into_iter().enumerate() {
            let seek_key = format!("{}{:08}", key, 0);
            let (res, found) = l.find_near(seek_key.as_bytes(), less, allow_equal);
            if exp.is_none() {
                assert!(!found, "{}", i);
                continue;
            }
            let e = format!("{}{:08}", exp.unwrap(), 0);
            assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
        }
    }

    #[test]
    fn test_empty() {
        let key = b"aaa";
        let skl = new_test_skl();
        for less in &[false, true] {
            for allow_equal in &[false, true] {
                let (node, found) = skl.find_near(key, *less, *allow_equal);
                assert!(node.is_null());
                assert!(!found);
            }
        }
        let mut iter = InlineSkiplistIterator::new(skl.clone());
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
        iter.seek(key);
        assert!(!iter.valid());
    }

    #[test]
    fn test_basic() {
        let c = BytewiseComparator::default();
        let arena = OffsetArena::with_capacity(1 << 20);
        let list = InlineSkipList::new(c, arena);
        let table = vec!["key1", "key2", "key3", "key4", "key5"];

        for key in table.clone() {
            list.put(key.as_bytes());
        }
        assert_eq!(list.len(), 5);
        assert!(!list.is_empty());
        let mut iter = InlineSkiplistIterator::new(list);
        for key in &table {
            iter.seek(key.as_bytes());
            assert_eq!(iter.key(), key.as_bytes());
        }
        for key in table.iter().rev() {
            assert_eq!(iter.key(), key.as_bytes());
            iter.prev();
        }
        assert!(!iter.valid());
        iter.seek_to_first();
        for key in table.iter() {
            assert_eq!(iter.key(), key.as_bytes());
            iter.next();
        }
        assert!(!iter.valid());
        iter.seek_to_first();
        assert_eq!(iter.key(), table.first().unwrap().as_bytes());
        iter.seek_to_last();
        assert_eq!(iter.key(), table.last().unwrap().as_bytes());
    }

    fn test_concurrent_basic(n: usize, cap: usize, key_len: usize) {
        let cmp = BytewiseComparator::default();
        let arena = OffsetArena::with_capacity(cap);
        let skl = InlineSkipList::new(cmp, arena);
        let keys: Vec<_> = (0..n)
            .map(|i| format!("{1:00$}", key_len, i).to_owned())
            .collect();
        let (tx, rx) = mpsc::channel();
        for key in keys.clone() {
            let tx = tx.clone();
            let l = skl.clone();
            thread::Builder::new()
                .name("write thread".to_owned())
                .spawn(move || {
                    l.put(key);
                    tx.send(()).unwrap();
                })
                .unwrap();
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        for key in keys {
            let tx = tx.clone();
            let l = skl.clone();
            thread::Builder::new()
                .name("read thread".to_owned())
                .spawn(move || {
                    let mut iter = InlineSkiplistIterator::new(l);
                    iter.seek(key.as_bytes());
                    assert_eq!(iter.key(), key.as_bytes());
                    tx.send(()).unwrap();
                })
                .unwrap();
        }
        for _ in 0..n {
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        assert_eq!(skl.len(), n);
    }

    #[test]
    fn test_concurrent_basic_small_value() {
        test_concurrent_basic(1000, 1 << 20, 5);
    }
    #[test]
    fn test_concurrent_basic_big_value() {
        test_concurrent_basic(100, 120 << 20, 10);
    }
}
