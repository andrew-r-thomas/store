use std::{
    ops::Deref,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

pub struct AtomicArc<T> {
    ptr: AtomicPtr<AtomicArcInner<T>>,
}
pub struct AtomicArcInner<T> {
    rc: AtomicUsize,
    data: T,
}

impl<T> AtomicArc<T> {
    pub fn new(data: T) -> Self {
        let boxed = Box::new(AtomicArcInner {
            rc: AtomicUsize::new(1),
            data,
        });
        Self {
            ptr: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }

    pub fn compare_exchange(&self) {}
}
unsafe impl<T: Send + Sync> Send for AtomicArc<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicArc<T> {}

impl<T> Clone for AtomicArc<T> {
    fn clone(&self) -> Self {
        let inner = unsafe { self.ptr.load(Ordering::SeqCst).as_mut() }.unwrap();
        let old_rc = inner.rc.fetch_add(1, Ordering::SeqCst);
        if old_rc >= isize::MAX as usize {
            std::process::abort()
        }
        Self {
            ptr: AtomicPtr::new(inner),
        }
    }
}

impl<T> Deref for AtomicArc<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &unsafe { self.ptr.load(Ordering::SeqCst).as_ref() }
            .unwrap()
            .data
    }
}

impl<T> Drop for AtomicArc<T> {
    fn drop(&mut self) {
        todo!()
    }
}
