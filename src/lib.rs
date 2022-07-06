//! Content Archive codec.

use core::convert::{TryFrom, TryInto};
use std::io::{Cursor, Read, Seek};

use byteorder::{ByteOrder, LittleEndian};
use cid::Cid;
use thiserror::Error;
use unsigned_varint::io::read_u64 as varint_read_u64;

use libipld::{prelude::Codec, Block, DefaultParams, Ipld};
use libipld_cbor::DagCborCodec;

const HEADER_LENGTH: usize = 40;
const CHARACTERISTICS_LENGTH: usize = 16;

/// An IPLD Content Archive
#[derive(Debug, Clone)]
pub enum ContentArchive {
    V1(CarV1),
    V2(CarV2),
}

/// An IPLD Content Archive Version 1
#[derive(Debug, Clone)]
pub struct CarV1 {
    header: CarHeaderV1,
    blocks: Vec<Block<DefaultParams>>,
}

/// An IPLD Content Archive Version 2; wraps a CAR Version 1
#[derive(Debug, Clone)]
pub struct CarV2 {
    header: CarHeaderV2,
    car_v1: CarV1,
    index: Option<CarV2Index>,
}

/// An IPLD Content Archive Header
#[derive(Debug, Clone)]
pub enum CarHeader {
    V1(CarHeaderV1),
    V2(CarHeaderV2),
}

/// An IPLD Content Archive Header Version 1
#[derive(Debug, Clone)]
pub struct CarHeaderV1 {
    pub roots: Vec<Cid>,
}

/// An IPLD Content Archive Header Version 2
#[derive(Debug, Clone)]
pub struct CarHeaderV2 {
    characteristics: [u8; CHARACTERISTICS_LENGTH],
    data_offset: u64,
    data_size: u64,
    index_offset: u64,
}

#[derive(Debug, Clone)]
pub struct CarV2Index;

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
    Cid(#[from] cid::Error),

    /// Error while decoding Varint
    #[error(transparent)]
    VarintDecode(#[from] unsigned_varint::io::ReadError),
}

/// CAR result.
pub type CarResult<T> = Result<T, CarError>;

impl CarV1 {
    pub fn new(header: CarHeaderV1, blocks: Vec<Block<DefaultParams>>) -> Self {
        Self { header, blocks }
    }
}

impl CarV2 {
    pub fn new(header: CarHeaderV2, car_v1: CarV1, index: Option<CarV2Index>) -> Self {
        Self {
            header,
            car_v1,
            index,
        }
    }

    pub fn is_fully_indexed(&self) -> bool {
        self.header.characteristics[0] & 0b1000_0000 == 1
    }
}

impl ContentArchive {
    pub fn read_bytes<R: Read + Seek>(mut r: R) -> CarResult<ContentArchive> {
        let header = read_header(&mut r)?;
        match header {
            CarHeader::V1(header) => {
                Ok(ContentArchive::V1(CarV1::new(header, read_car_v1_data(r)?)))
            }
            CarHeader::V2(header) => {
                r.seek(std::io::SeekFrom::Start(header.data_offset))?;
                let mut car_v1_buf = vec![0u8; header.data_size as usize];
                r.read_exact(&mut car_v1_buf)?;
                let mut reader = Cursor::new(car_v1_buf);
                let index_offset = header.index_offset;
                Ok(ContentArchive::V2(CarV2::new(
                    header,
                    ContentArchive::read_bytes(&mut reader)?.try_into()?,
                    read_v2_index(&mut r, index_offset)?,
                )))
            }
        }
    }
}

impl TryFrom<ContentArchive> for CarV1 {
    type Error = CarError;

    fn try_from(value: ContentArchive) -> CarResult<Self> {
        match value {
            ContentArchive::V1(car) => Ok(car),
            _ => Err(CarError::InvalidFormat),
        }
    }
}

fn read_header<R: Read>(mut r: R) -> CarResult<CarHeader> {
    let header_length = varint_read_u64(&mut r)?;

    let mut header_buf = vec![0; header_length as usize];
    r.read_exact(&mut header_buf)?;

    let header_map: Ipld = DagCborCodec.decode(&header_buf)?;
    match header_map.get("version") {
        Ok(Ipld::Integer(version)) => match &version {
            1 => Ok(CarHeader::V1(parse_v1_header(header_map)?)),
            2 => {
                let mut v2_header_buf = [0; HEADER_LENGTH];
                r.read_exact(&mut v2_header_buf)?;
                Ok(CarHeader::V2(parse_v2_header(v2_header_buf)?))
            }
            _ => Err(CarError::UnsupportedVersion(*version as u8)),
        },
        _ => return Err(CarError::InvalidFormat),
    }
}

fn parse_v2_header(header: [u8; HEADER_LENGTH]) -> CarResult<CarHeaderV2> {
    Ok(CarHeaderV2 {
        characteristics: header[0..CHARACTERISTICS_LENGTH].try_into()?,
        data_offset: LittleEndian::read_u64(
            &header[CHARACTERISTICS_LENGTH..CHARACTERISTICS_LENGTH + 8],
        ),
        data_size: LittleEndian::read_u64(
            &header[CHARACTERISTICS_LENGTH + 8..CHARACTERISTICS_LENGTH + 16],
        ),
        index_offset: LittleEndian::read_u64(&header[CHARACTERISTICS_LENGTH + 16..HEADER_LENGTH]),
    })
}

fn parse_v1_header(header_map: Ipld) -> CarResult<CarHeaderV1> {
    let roots = match header_map.get("roots") {
        Ok(Ipld::List(cids)) => cids
            .iter()
            .map(|ipld| match ipld {
                Ipld::Link(link) => Ok(link.clone()),
                _ => Err(CarError::InvalidFormat),
            })
            .collect::<Result<Vec<_>, _>>(),
        _ => Err(CarError::InvalidFormat),
    }?;

    Ok(CarHeaderV1 { roots })
}

fn read_car_v1_data<R: Read>(mut r: R) -> CarResult<Vec<Block<DefaultParams>>> {
    let mut data: Vec<Block<DefaultParams>> = vec![];
    while let Ok(length) = varint_read_u64(&mut r) {
        let mut data_buf = vec![0u8; length as usize];
        r.read_exact(&mut data_buf)?;
        let mut data_stream = Cursor::new(data_buf);

        let cid = Cid::read_bytes(&mut data_stream)?;
        let pos = data_stream.position() as usize;
        let data_buf = data_stream.into_inner();
        let block = Block::new(cid, data_buf[pos..].to_vec())?;
        data.push(block);
    }
    Ok(data)
}

// TODO: Finish index parsing
fn read_v2_index<R: Read + Seek>(mut r: R, index_offset: u64) -> CarResult<Option<CarV2Index>> {
    if index_offset == 0 {
        return Ok(None);
    }
    r.seek(std::io::SeekFrom::Start(index_offset))?;

    let codec = varint_read_u64(&mut r)?;

    match codec {
        0x0400 => (), // TODO: IndexSorted
        0x0401 => (), // TODO: MultihashIndexSorted
        _ => (),
    }

    Ok(Some(CarV2Index {}))
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
                    vec![Cid::from_str("QmfEoLyB5NndqeKieExd1rtJzTduQUPEV8TwAYcUiy3H5Z").unwrap(),]
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
