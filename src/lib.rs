//! Content Archive codec.

pub mod v1;
pub mod v2;

use core::convert::{TryFrom, TryInto};
use std::io::{Cursor, Read, Seek};

use thiserror::Error;
use unsigned_varint::io::read_u64 as varint_read_u64;

use libipld::{prelude::Codec, Ipld, cbor::DagCborCodec};
use crate::v1::{CarHeaderV1, CarV1};

const HEADER_LENGTH: usize = 40;
const CHARACTERISTICS_LENGTH: usize = 16;

/// An IPLD Content Archive
#[derive(Debug, Clone)]
pub enum ContentArchive {
    V1(v1::CarV1),
    V2(v2::CarV2),
}

/// An IPLD Content Archive Header
#[derive(Debug, Clone)]
pub enum CarHeader {
    V1(v1::CarHeaderV1),
    V2(v2::CarHeaderV2),
}

// TODO: Richer errors
/// CAR error.
#[derive(Debug, Error)]
pub enum CarError {
    /// Found unsupported version while reading car header.
    #[error("Unsupported CAR Version: {0}")]
    UnsupportedVersion(u8),

    /// Invalid Content Archive format.
    #[error("Invalid Content Archive Format")]
    InvalidFormat,

    /// Ipld error.
    #[error(transparent)]
    Ipld(#[from] libipld::error::Error),

    /// Io error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Caused by converting `characteristics` slice into owned array.
    #[error(transparent)]
    CharacteristicsConversion(#[from] core::array::TryFromSliceError),

    /// Utf8 error.
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),

    /// Cid error.
    #[error(transparent)]
    Cid(#[from] libipld::cid::Error),

    /// Error while decoding Varint
    #[error(transparent)]
    VarintDecode(#[from] unsigned_varint::io::ReadError),
}

/// CAR result.
pub type CarResult<T> = Result<T, CarError>;

fn read_header<R: Read>(mut r: R) -> CarResult<CarHeader> {
    let header_length = varint_read_u64(&mut r)?;

    let mut header_buf = vec![0; header_length as usize];
    r.read_exact(&mut header_buf)?;

    let header_map: Ipld = DagCborCodec.decode(&header_buf)?;

    match header_map.get("version") {
        Ok(Ipld::Integer(version)) => match &version {
            2 => {
                let mut v2_header_buf = [0; HEADER_LENGTH];
                r.read_exact(&mut v2_header_buf)?;
                Ok(CarHeader::V2(v2::parse_v2_header(v2_header_buf)?))
            }
            _ => Err(CarError::UnsupportedVersion(*version as u8)),
        },
        _ => return Err(CarError::InvalidFormat),
    }
}


impl ContentArchive {
    pub fn read_bytes<R: Read + Seek>(mut r: R) -> CarResult<ContentArchive> {
        CarV1::from_reader(&mut r).map(ContentArchive::V1).or_else(|_| {
            r.seek(std::io::SeekFrom::Start(0))?;
            let header = read_header(&mut r)?;

            match header {
                CarHeader::V2(header) => {
                    r.seek(std::io::SeekFrom::Start(header.data_offset))?;
                    let mut car_v1_buf = vec![0u8; header.data_size as usize];
                    r.read_exact(&mut car_v1_buf)?;
                    let mut reader = Cursor::new(car_v1_buf);
                    let index_offset = header.index_offset;
                    Ok(ContentArchive::V2(v2::CarV2::new(
                        header,
                        ContentArchive::read_bytes(&mut reader)?.try_into()?,
                        v2::read_v2_index(&mut r, index_offset)?,
                    )))
                }
                _ => Err(CarError::InvalidFormat),
            }
        })
    }
}

impl TryFrom<ContentArchive> for v1::CarV1 {
    type Error = CarError;

    fn try_from(value: ContentArchive) -> CarResult<Self> {
        match value {
            ContentArchive::V1(car) => Ok(car),
            _ => Err(CarError::InvalidFormat),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    struct Fixture {
        pub source: PathBuf,
    }

    impl Fixture {
        pub fn new<P: AsRef<Path>>(filename: P) -> Self {
            let root_dir = &std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
            let mut source = PathBuf::from(root_dir);
            source.push("tests/fixtures");
            source.push(&filename);
            Self { source }
        }
    }

    #[test]
    fn it_reads_car_v2() {
        let car = std::fs::read(Fixture::new("carv2-basic.car").source).unwrap();
        let decoded_car = ContentArchive::read_bytes(&mut Cursor::new(car)).unwrap();

        match decoded_car {
            ContentArchive::V1(_) => panic!("Expected V2"),
            ContentArchive::V2(carv2) => {
                assert_eq!(carv2.header.data_offset, 51);
                assert_eq!(carv2.header.data_size, 448);
                assert_eq!(carv2.header.index_offset, 499);
                assert!(carv2.index.is_some());
                assert!(!carv2.is_fully_indexed());

                assert_eq!(
                    carv2.car_v1.header.roots,
                    vec![libipld::cid::Cid::from_str("QmfEoLyB5NndqeKieExd1rtJzTduQUPEV8TwAYcUiy3H5Z").unwrap(),]
                );
                assert_eq!(carv2.car_v1.blocks.len(), 5);
            }
        }
    }

    #[test]
    fn it_reads_car_v1() {
        let car = std::fs::read(Fixture::new("carv1-basic.car").source).unwrap();
        let decoded_car = ContentArchive::read_bytes(&mut Cursor::new(car)).unwrap();
        match decoded_car {
            ContentArchive::V2(_) => panic!("Expected V1"),
            ContentArchive::V1(carv1) => {
                assert_eq!(carv1.header.roots.len(), 1);
            }
        }
    }
}
