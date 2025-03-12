use std::marker::PhantomData;

use crate::pager::{Page, Pager};

pub struct BTree<K>
where
    K: for<'k> PartialOrd<&'k [u8]>,
{
    _ph: PhantomData<K>,
    pager: Pager,
    root_page_id: u32,
    page_stack: Vec<u32>,
}
impl<K> BTree<K>
where
    K: for<'k> PartialOrd<&'k [u8]>,
{
    pub fn new(pager: Pager, root_page_id: u32) -> Self {
        Self {
            _ph: PhantomData,
            pager,
            root_page_id,
            page_stack: Vec::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&[u8]> {
        self.find_leaf(key);

        let leaf_page = self.pager.get(*self.page_stack.last().unwrap()).unwrap();
        if let Some(slot_num) = Self::search_page(key, &leaf_page) {
            let slot = leaf_page.get_slot(slot_num);
            let key_len = u32::from_be_bytes(slot[0..4].try_into().unwrap()) as usize;
            let val_len =
                u32::from_be_bytes(slot[4 + key_len..8 + key_len].try_into().unwrap()) as usize;
            return Some(&slot[8 + key_len..8 + key_len + val_len]);
        }
        None
    }
    pub fn set(&mut self) {}
    pub fn delete(&mut self) {}

    fn find_leaf(&mut self, key: &K) {
        self.page_stack.clear();

        let mut current_page_id = self.root_page_id;
        loop {
            self.page_stack.push(current_page_id);
            let current_page = self.pager.get(current_page_id).unwrap();
            if current_page.level() == 0 {
                break;
            }
            match Self::search_page(key, current_page) {
                Some(slot_num) => {
                    let slot = current_page.get_slot(slot_num);
                    let key_len = u32::from_be_bytes(slot[0..4].try_into().unwrap()) as usize;
                    let page_id =
                        u32::from_be_bytes(slot[4 + key_len..4 + key_len + 4].try_into().unwrap());
                    current_page_id = page_id;
                }
                None => {
                    // go right
                    current_page_id = current_page.right_ptr();
                }
            }
        }
    }

    // TODO: two different functions for finding pointer vs finding value
    fn search_page(target: &K, page: &Page) -> Option<usize> {
        for (slot, slot_num) in page.iter_slots().zip(0..) {
            let key_len = u32::from_be_bytes(slot[0..4].try_into().unwrap()) as usize;
            let key = &slot[4..4 + key_len];
            if target < &key {
                return Some(slot_num);
            }
        }
        None
    }
}
