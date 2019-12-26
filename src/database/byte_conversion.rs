use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{mem, io};

pub trait ByteConversion: Sized {
    fn write_bytes_to<B>(self, but: &mut B) -> Result<(), io::Error>
    where
        B: WriteBytesExt;

    fn to_bytes(self) -> Vec<u8> {
        let mut buf = vec![];
        self.write_bytes_to(&mut buf).unwrap();
        buf
    }

    fn from_bytes(bytes: Vec<u8>) -> Self;
}

impl ByteConversion for i64 {
    fn write_bytes_to<B>(self, buf: &mut B) -> Result<(), io::Error>
    where
        B: WriteBytesExt,
    {
        buf.write_i64::<BigEndian>(self)?;
        Ok(())
    }

    fn from_bytes(buf: Vec<u8>) -> Self {
        assert_eq!(buf.len(), mem::size_of::<Self>());
        buf.as_slice().read_i64::<BigEndian>().unwrap()
    }
}

impl ByteConversion for String {
    fn write_bytes_to<B>(self, buf: &mut B) -> Result<(), io::Error>
    where
        B: WriteBytesExt,
    {
        let bytes = self.into_bytes();
        for byte in bytes {
            buf.write_u8(byte)?;
        }
        Ok(())
    }

    fn from_bytes(buf: Vec<u8>) -> Self {
        String::from_utf8(buf).unwrap()
    }
}
