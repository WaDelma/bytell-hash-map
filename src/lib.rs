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

pub struct FlatHashMap<K, V, H> {
    ptr: *mut Cell<K, V>,
    capacity: usize,
    hasher: H
}

impl<K, V, H> FlatHashMap<K, V, H>
    where K: Hash + PartialEq,
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

    // TODO: Reallocate if when load factor is surpassed
    pub fn insert(&mut self, key: K, value: V) -> Option<(K, V)> {
        let mut hash = self.hash(&key) as usize;
        unsafe {
            let mut cur_meta = &mut Metadata::default();
            let mut data_ptr = ptr::null_mut();
            self.mut_data(hash, &mut cur_meta, &mut data_ptr);

            if cur_meta.is_empty() {
                cur_meta.set_storage(false);
                cur_meta.set_jump(0);
                ptr::write(data_ptr, Entry {
                    key,
                    value
                });
                return None;
            } else if cur_meta.is_storage() {
                // TODO: Relocate linked list
                unimplemented!();
            }
            loop {
                let data = &mut *data_ptr;
                if data.key == key {
                    let value_ptr = (&mut data.value) as *mut _;
                    return Some((key, ptr::replace(value_ptr, value)));
                }
                let jump = cur_meta.jump_length();
                if jump == 0 {
                    let mut jumps = 0;
                    let prev_meta = cur_meta;
                    let mut cur_meta = &mut Metadata::default();
                    loop {
                        if jumps == 127 {
                            // TODO: Too many jumps: reallocate and try again
                            unimplemented!();
                        }
                        jumps += 1;
                        hash += 1; // TODO: Use jump table

                        self.mut_data(hash, &mut cur_meta, &mut data_ptr);

                        if cur_meta.is_empty() {
                            cur_meta.set_storage(true);
                            cur_meta.set_jump(0);
                            prev_meta.set_jump(jumps);
                            ptr::write(data_ptr, Entry {
                                key,
                                value
                            });
                            return None;
                        }
                    }
                }
                hash += jump as usize; // TODO: Use jump table

                self.mut_data(hash, &mut cur_meta, &mut data_ptr);
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut hash = self.hash(&key) as usize;
        unsafe {
            let mut cur_meta = &Metadata::default();
            let mut data_ptr = ptr::null();
            self.get_data(hash, &mut cur_meta, &mut data_ptr);

            if cur_meta.is_storage() {
                return None;
            }
            loop {
                let data = &*data_ptr;
                if &data.key == key {
                    return Some(&data.value);
                }
                let jump = cur_meta.jump_length();
                if jump == 0 {
                    return None;
                }
                hash += jump as usize; // TODO: Use jump table
                self.get_data(hash, &mut cur_meta, &mut data_ptr);
            }
        }
    }

    // TODO: Abstract over mutability
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut hash = self.hash(&key) as usize;
        unsafe {
            let mut cur_meta = &mut Metadata::default();
            let mut data_ptr = ptr::null_mut();
            self.mut_data(hash, &mut cur_meta, &mut data_ptr);

            if cur_meta.is_storage() {
                return None;
            }
            loop {
                let data = &mut *data_ptr;
                if &data.key == key {
                    return Some(&mut data.value);
                }
                let jump = cur_meta.jump_length();
                if jump == 0 {
                    return None;
                }
                hash += jump as usize; // TODO: Use jump table
                self.mut_data(hash, &mut cur_meta, &mut data_ptr);
            }
        }
    }

    fn hash(&self, key: &K) -> u64 {
        let mut state = self.hasher.build_hasher();
        key.hash(&mut state);
        state.finish()
    }

    fn get_data(&self, hash: usize, cur_meta: &mut &Metadata, data_ptr: &mut *const Entry<K, V>) {
        unsafe {
            let (cell, slot) = split_hash(hash, self.capacity);
            let cur_cell = self.ptr.offset(cell);

            *cur_meta = &(*cur_cell).meta.0[slot];
            
            let datum_ptr = (*cur_cell).data.0.as_ref().as_ptr();
            *data_ptr = datum_ptr.offset(slot as isize);
        }
    }
    
    // TODO: Abstract over mutability
    fn mut_data(&mut self, hash: usize, cur_meta: &mut &mut Metadata, data_ptr: &mut *mut Entry<K, V>) {
        unsafe {
            let (cell, slot) = split_hash(hash, self.capacity);
            let cur_cell = self.ptr.offset(cell);

            *cur_meta = &mut (*cur_cell).meta.0[slot];

            let datum_ptr = (*cur_cell).data.0.as_mut().as_mut_ptr();
            *data_ptr = datum_ptr.offset(slot as isize);
        }
    }
}

fn split_hash(hash: usize, capacity: usize) -> (isize, usize) {
    (
        ((hash >> 4) & capacity) as isize,
        hash & 15
    )
}

#[test]
fn adding_one_works() {
    use std::collections::hash_map::RandomState;
    let max = 1000;
    for n in 0..max {
        let mut map = FlatHashMap::new(RandomState::new());
        map.insert(n, n);
        assert_eq!(Some(&n), map.get(&n));
    }
}