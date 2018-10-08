use std::mem;
use std::ptr;
use std::hash::{BuildHasher, Hash, Hasher};

struct Metadata(u8);
impl Metadata {
    fn is_empty(&self) -> bool {
        self.0 == 0b11111111
    }
    fn is_storage(&self) -> bool {
        self.0 & 0b10000000 == 0b10000000
    }
    fn jump_length(&self) -> u8 {
        self.0 & 0b01111111
    }
    fn set_storage(&mut self, storage: bool) {
        if storage {
            self.0 |= 0b10000000;
        } else {
            self.0 &= 0b01111111;
        }
    }
    fn set_jump(&mut self, jump: u8) {
        assert!(jump & 0b10000000 == 0b00000000);
        self.0 &= 0b10000000;
        self.0 |= jump;
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata(0b11111111)
    }
}

#[derive(Default)]
struct Metadatum([Metadata; 16]);

struct Entry<K, V> {
    key: K,
    value: V
}

struct Datum<K, V>([Entry<K, V>; 16]);

struct Cell<K, V> {
    meta: Metadatum,
    data: Datum<K, V>
}

struct FlatHashMap<K, V, H> {
    ptr: *mut Cell<K, V>,
    capacity: usize,
    hasher: H
}

impl<K, V, H> FlatHashMap<K, V, H>
    where K: Hash,
          H: BuildHasher
{
    pub fn new(hasher: H) -> Self {
        let mut data = vec![Cell {
            meta: Metadatum::default(),
            data: unsafe { mem::zeroed() }, // TODO: Uninitialize
        }];
        let ptr = data.as_mut_ptr();
        mem::forget(data);
        FlatHashMap {
            ptr,
            capacity: 1,
            hasher
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<(K, V)> {
        let hash = self.hash(&key) as usize;
        let cell = ((hash >> 4) & self.capacity) as isize;
        let slot = hash & 15;

        unsafe {
            let mut cur_cell = self.ptr.offset(cell);
            let mut cur_meta = &mut (*cur_cell).meta.0[slot];
            if cur_meta.is_empty() {
                cur_meta.set_storage(false);
                cur_meta.set_jump(0);
                let datum_ptr = (*cur_cell).data.0.as_mut().as_mut_ptr();
                let data_ptr = datum_ptr.offset(slot as isize); 
                ptr::write(data_ptr, Entry {
                    key,
                    value
                });
                None
            } else cur_meta.is_storage() {
                // TODO: Relocate linked list
            } else {
                // TODO: Direct hit: Start following linked list and check if were already there. If not add to the end
            }
        }
        unimplemented!()
    }

    fn hash(&self, key: &K) -> u64 {
        let mut state = self.hasher.build_hasher();
        key.hash(&mut state);
        state.finish()
    }
}
