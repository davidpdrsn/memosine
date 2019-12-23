use std::ops::{Add, AddAssign, Sub, SubAssign};

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Pos(usize);

impl Pos {
    #[inline]
    pub fn value(self) -> usize {
        self.0
    }
}

impl Add<usize> for Pos {
    type Output = Pos;

    fn add(self, other: usize) -> Pos {
        Pos(self.0 + other)
    }
}

impl<'a> Add<usize> for &'a Pos {
    type Output = Pos;

    fn add(self, other: usize) -> Pos {
        *self + other
    }
}

impl Sub<usize> for Pos {
    type Output = Pos;

    fn sub(self, other: usize) -> Pos {
        Pos(self.0 - other)
    }
}

impl<'a> Sub<usize> for &'a Pos {
    type Output = Pos;

    fn sub(self, other: usize) -> Pos {
        *self - other
    }
}
