use std::cell::RefCell;
use std::mem;
use std::ptr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::sync::{Arc,Mutex};


//  多线程会并发调用allocate，因此arena需要并发安全的
const BLOCK_SIZE: usize = 4096;

pub trait Arena {
    /// 返回指向新分配的“chunk”字节内存块的起始指针
    unsafe fn allocate<T>(&self, chunk: usize, align: usize) -> *mut T;
    /// Return the size of memory that has been allocated.
    fn memory_used(&self) -> usize;
}

struct OffsetArenaInner {
    len: AtomicUsize,  // 起始偏移量
    cap: usize,        // arena容量
    ptr: *mut u8,      //分配在堆上的指针
}

#[derive(Clone)]
pub struct OffsetArena {
    inner: Arc<OffsetArenaInner>,
}

impl Drop for OffsetArenaInner {
    fn drop(&mut self) {
        // manully drop ArenaInner
        if !self.ptr.is_null() {
            unsafe {
                let ptr = self.ptr as *mut u64;
                let cap = self.cap / 8;
                Vec::from_raw_parts(ptr, 0, cap);
            }
        }
    }
}

impl Arena for OffsetArena {
    unsafe fn allocate<T>(&self, chunk: usize, align: usize) -> *mut T {
        match self.alloc(align, chunk) {
            Ok(offset) => self.get_mut(offset),
            Err(_) => ptr::null_mut()
        }
    }

    /// Return the size of memory that has been allocated.
    fn memory_used(&self) -> usize {
        self.inner.len.load(Ordering::SeqCst)
    }
}

unsafe impl Send for OffsetArena {}
unsafe impl Sync for OffsetArena {}

impl OffsetArena {
    // The real cap will be aligned with 8
    pub fn with_capacity(cap: usize) -> Self {
        let mut buf: Vec<u64> = Vec::with_capacity(cap / 8);
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * 8;
        // 防止内存释放  防止buf 离开作用域时自动释放内存
        mem::forget(buf);
        OffsetArena {
            inner: Arc::new(OffsetArenaInner {
                len: AtomicUsize::new(1),
                cap,
                ptr,
            }),
        }
    }

    // Allocates `size` bytes aligned with `align`
    //  返回指针偏移
    fn alloc(&self, align: usize, size: usize) -> Result<usize, String> {
        // 生成掩码
        let align_mask = align - 1;
        // 实际分配大小
        let size = size + align_mask;
        let offset = self.inner.len.fetch_add(size, Ordering::SeqCst);
        // [(offset + align_mask) / align] * align 等效  (offset + align_mask) & !align_mask;
        // 位与清除低位
        // 例如align 是 8（即 align_mask 是 7，二进制表示为 0b0111）
        // offset 为 5（二进制 0b0101）
        // (offset + align_mask) 计算为 12（二进制 0b1100）。
        // & !align_mask 清除低三位，结果是 8（二进制 0b1000），这是最近的8的倍数。

        // buf[T,T,T...]
        //       |
        //       对应指针
        // 获取指针后对这个T类型赋值
        // 计算对齐后的偏移量 向下取整到 align 的倍数
        let ptr_offset = (offset + align_mask) & !align_mask;

        if offset + size > self.inner.cap {
            return Err("Not enough memory available".to_string());
        }

        Ok(ptr_offset)
    }


    // Returns a raw pointer with given arena offset
    // 通过偏移获取对象指针
    unsafe fn get_mut<T>(&self, offset: usize) -> *mut T {
        if offset == 0 {
            ptr::null_mut()
        } else {
            self.inner.ptr.add(offset) as *mut T
        }
    }
}






/// `BlockArena` 是一个线程安全的内存池，用于动态分配和管理 Node 内存。
#[derive(Clone)]
pub struct BlockArena {
    inner: Arc<BlockArenaInner>,
}

/// `BlockArenaInner` 详细实现了内存分配机制。
/// 它维护了一组动态分配的内存块，并记录总的内存使用量。
/// todo 是否使用len来标识offset？
pub struct BlockArenaInner {
    ptr: AtomicPtr<u8>, // 剩余未分配的指针
    bytes_remaining: AtomicUsize, // 剩余空间大小
    blocks: RefCell<Vec<Vec<u8>>>, // 存储所有内存块的容器
    memory_usage: AtomicUsize, // 总内存使用量
    allocation_lock: Mutex<()>,
}

impl Default for BlockArena {
    fn default() -> Self {
        BlockArena {
            inner: Arc::new(BlockArenaInner{
                ptr: AtomicPtr::new(null_mut()),
                bytes_remaining: AtomicUsize::new(0),
                blocks: RefCell::new(Vec::<Vec<u8>>::default()),
                memory_usage: AtomicUsize::new(0),
                allocation_lock: Mutex::new(())
            }),
        }
    }
}

impl BlockArena {
    fn allocate_fallback(&self, size: usize) -> *mut u8 {
        // 是否超过了块大小的四分之一
        if size > BLOCK_SIZE / 4 {
            //对象的大小超过块大小的四分之一。单独分配以避免浪费太多剩余空间。
            return self.allocate_new_block(size);
        }


        let new_block_ptr = self.allocate_new_block(BLOCK_SIZE);
        unsafe {
            let ptr = new_block_ptr.add(size);
            self.inner.ptr.store(ptr, Ordering::Release);
        };

        self.inner.bytes_remaining
            .store(BLOCK_SIZE - size, Ordering::Release);
        new_block_ptr
    }
    fn allocate_new_block(&self, block_bytes: usize) -> *mut u8 {
        let mut new_block = vec![0; block_bytes];
        let p = new_block.as_mut_ptr();
        // 该 Vec 保存到 blocks 中以保证内存不会被释放
        self.inner.blocks.borrow_mut().push(new_block);
        self.inner.memory_usage.fetch_add(block_bytes, Ordering::Relaxed);
        p
    }
}
// 存在一个问题，分配一个小的还剩 1024 分配 1016，就会去分配一个重新分配块，浪费1024byte空间
impl Arena for BlockArena {
    unsafe fn allocate<T>(&self, chunk: usize, align: usize) -> *mut T {
        // 内存块大小
        assert!(chunk > 0);
        let ptr_size = mem::size_of::<usize>();
        let align_mask = align - 1;
        // 检查对齐要求 为2的幂
        assert_eq!(align & align_mask, 0);
        // 计算对齐偏差
        let slop = (align - (self.inner.ptr.load(Ordering::Acquire) as usize & align_mask)) % align;

        // 计算所需的总空间
        let needed = chunk + slop;
        // 扩容内存分配
        loop {
            let current_bytes_remaining = self.inner.bytes_remaining.load(Ordering::Acquire);
            if needed <= current_bytes_remaining {
                let current_ptr = self.inner.ptr.load(Ordering::Acquire);
                let adjusted_ptr = current_ptr.add(slop);
                let new_ptr = adjusted_ptr.add(chunk);
                let new_bytes_remaining = current_bytes_remaining - needed;
                match self.inner.allocation_lock.try_lock() {
                    Ok(lock) => {
                        if self.inner.bytes_remaining.compare_exchange(current_bytes_remaining, new_bytes_remaining, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                            self.inner.ptr.store(new_ptr, Ordering::Release);
                            assert_eq!(
                                adjusted_ptr as usize & align_mask,
                                0,
                                "allocated memory should be aligned with {}",
                                ptr_size
                            );
                            return adjusted_ptr as *mut T;
                        }
                    }
                    Err(_) => {
                    }
                }
            } else {
                let _lock = self.inner.allocation_lock.lock().unwrap();
                let current_bytes_remaining = self.inner.bytes_remaining.load(Ordering::Acquire);
                if  needed  >  current_bytes_remaining {
                    let new_ptr = self.allocate_fallback(chunk) as *mut T;
                    assert_eq!(
                        new_ptr as usize & (align - 1),
                        0,
                        "allocated memory should be aligned with {}",
                        ptr_size
                    );
                    return new_ptr
                }
            }
        };
    }

    #[inline]
    fn memory_used(&self) -> usize {
        self.inner.memory_usage.load(Ordering::Acquire)
    }
}

unsafe impl Send for BlockArena {}
unsafe impl Sync for BlockArena {}


#[cfg(test)]
mod tests {
    use crate::mem::arena::{ Arena, BlockArena, OffsetArena, BLOCK_SIZE};
    use rand::Rng;
    use std::{mem, ptr};
    use std::sync::atomic::Ordering;
    use std::thread;
    #[test]
    fn test_new_arena() {
        let a = BlockArena::default();
        assert_eq!(a.memory_used(),0);
        assert_eq!(a.inner.bytes_remaining.load(Ordering::Acquire), 0);
        assert_eq!(a.inner.ptr.load(Ordering::Acquire), ptr::null_mut());
        assert_eq!(a.inner.blocks.borrow_mut().len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_allocate_empty_should_panic() {
        let a = BlockArena::default();
        unsafe { a.allocate::<u8>(0, 0) };
    }

    #[test]
    fn test_allocate_new_block() {
        let a = BlockArena::default();
        let mut expect_size = 0;
        for (i, size) in [1, 128, 256, 1000, 4096, 10000].iter().enumerate() {
            a.allocate_new_block(*size);
            expect_size += *size;
            assert_eq!(a.memory_used(), expect_size, "memory used should match");
            assert_eq!(
                a.inner.blocks.borrow().len(),
                i + 1,
                "number of blocks should match"
            )
        }
    }

    #[test]
    fn test_allocate_fallback() {
        let a = BlockArena::default();
        assert_eq!(a.memory_used(), 0);
        a.allocate_fallback(1);
        assert_eq!(a.memory_used(), BLOCK_SIZE);
        assert_eq!(a.inner.bytes_remaining.load(Ordering::Acquire), BLOCK_SIZE - 1);
        a.allocate_fallback(BLOCK_SIZE / 4 + 1);
        assert_eq!(a.memory_used(), BLOCK_SIZE + BLOCK_SIZE / 4 + 1);
    }

    #[test]
    fn test_allocate_mixed() {
        let a = BlockArena::default();
        let mut allocated = vec![];
        let mut allocated_size = 0;
        let n = 10000;
        let mut r = rand::thread_rng();
        for i in 0..n {
            let size = if i % (n / 10) == 0 {
                if i == 0 {
                    continue;
                }
                i
            } else {
                if i == 1 {
                    1
                } else {
                    r.gen_range(1, i)
                }
            };
            let ptr = unsafe { a.allocate::<u8>(size, 8) };
            unsafe {
                for j in 0..size {
                    let np = ptr.add(j);
                    (*np) = (j % 256) as u8;
                }
            }
            allocated_size += size;
            allocated.push((ptr, size));
            assert!(
                a.memory_used() >= allocated_size,
                "the memory used {} should be greater or equal to expecting allocated {}",
                a.memory_used(),
                allocated_size
            );
        }
        for (ptr, size) in allocated.iter() {
            unsafe {
                for i in 0..*size {
                    let p = ptr.add(i);
                    assert_eq!(*p, (i % 256) as u8);
                }
            }
        }
    }
          
    #[derive(Debug)]
    #[repr(C)]  // 指定结构体以8字节对齐
    struct AlignedStruct {
        data: [u8; 64]
    }

    #[test]
    fn test_offset_arena_concurrency() {

        let arena = OffsetArena::with_capacity(1 << 20);
        let arena_clone = arena.clone();
        let mut handles = vec![];
        let align = mem::align_of::<AlignedStruct>();
        for _ in 0..10 { // 创建10个并发线程
            let arena_clone = arena_clone.clone();
            let handle = thread::spawn(move || {
        
                // 每个线程尝试分配1000次，每次分配64字节内存，对齐8字节
                for _ in 0..1000 {
                    unsafe {
                        let ptr = unsafe { arena_clone.allocate::<AlignedStruct>(64, align)};
                        if !ptr.is_null() {
                            // 假设我们分配的是u8类型的数组，可以写入数据以验证
                            for i in 0..64 {
                                (&mut *ptr).data[i]=0xaa // 使用0xAA填充内存
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }
    
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 最后验证分配的内存量是否符合预期
        assert_eq!(arena.memory_used(), 10 * 1000 * mem::size_of::<AlignedStruct>() + 1 );
    }

    #[test]
    fn test_block_arena_concurrency() {
        let arena = BlockArena::default();
     
        let arena_clone = arena.clone();
     
        let mut handles = vec![];
        let align = mem::align_of::<AlignedStruct>();
        for _ in 0..10 { // 创建10个并发线程
            let arena_clone = arena_clone.clone();
 
            let handle = thread::spawn(move || {
        
                // 每个线程尝试分配1000次，每次分配64字节内存，对齐8字节
                for _ in 0..1000 {
                    unsafe {
                        let ptr = unsafe { arena_clone.allocate::<AlignedStruct>(64, align)};
                        if !ptr.is_null() {
                            // 假设我们分配的是u8类型的数组，可以写入数据以验证
                            for i in 0..64 {
                                (&mut *ptr).data[i]=0xaa // 使用0xAA填充内存
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }
    
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        // // 最后验证分配的内存量是否符合预期
        assert_eq!(arena.memory_used(), 643072 );
    }
}
