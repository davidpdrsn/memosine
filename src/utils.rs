use crate::sql::Ident;
use extend::ext;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::ops::Index;
use std::ops::{Deref, DerefMut};

pub mod parse;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Either<A, B> {
    A(A),
    B(B),
}

#[derive(Debug, Default)]
pub struct IdMap<V>
where
    V: Id,
    V::Id: Hash + Eq,
{
    map: HashMap<V::Id, V>,
}

impl<V> IdMap<V>
where
    V: Id,
    V::Id: Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, t: V) {
        self.map.insert(t.id(), t);
    }

    pub fn get(&self, k: &V::Id) -> Option<&V> {
        self.map.get(k)
    }

    pub fn get_mut(&mut self, k: &V::Id) -> Option<&mut V> {
        self.map.get_mut(k)
    }

    pub fn contains_key(&self, k: &V::Id) -> bool {
        self.map.contains_key(k)
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<V::Id, V> {
        self.map.iter()
    }
}

impl<V> FromIterator<V> for IdMap<V>
where
    V: Id,
    V::Id: Hash + Eq,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = V>,
    {
        Self {
            map: iter.into_iter().map(|v| (v.id(), v)).collect(),
        }
    }
}

pub trait Id {
    type Id;

    fn id(&self) -> Self::Id;
}

impl Id for Ident {
    type Id = Self;

    fn id(&self) -> Self::Id {
        self.clone()
    }
}

#[derive(Debug)]
pub struct Arena<T> {
    items: Vec<T>,
}

impl<T> Arena<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, item: T) -> usize {
        self.items.push(item);
        self.items.len() - 1
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}

impl<T> Index<usize> for Arena<T> {
    type Output = T;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.items[idx]
    }
}

#[ext(pub, name = ExtendOrSet)]
impl<T> Vec<T> {
    fn extend_or_set<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        if self.is_empty() {
            *self = iter.into_iter().collect();
        } else {
            self.extend(iter)
        }
    }
}

#[macro_export]
macro_rules! dot {
    ($name:ident) => {
        |x| x.$name()
    };
}
