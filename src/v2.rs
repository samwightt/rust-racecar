use std::io::{Read, Seek};
use byteorder::{LittleEndian, ByteOrder};
use crate::{v1, CarResult, CHARACTERISTICS_LENGTH, HEADER_LENGTH};
use unsigned_varint::io::read_u64 as varint_read_u64;

/// An IPLD Content Archive Version 2; wraps a CAR Version 1
#[derive(Debug, Clone)]
pub struct CarV2 {
    pub header: CarHeaderV2,
    pub car_v1: v1::CarV1,
    pub index: Option<CarV2Index>,
}

#[derive(Debug, Clone)]
pub struct CarV2Index;

/// An IPLD Content Archive Header Version 2
#[derive(Debug, Clone)]
pub struct CarHeaderV2 {
    pub characteristics: [u8; CHARACTERISTICS_LENGTH],
    pub data_offset: u64,
    pub data_size: u64,
    pub index_offset: u64,
}

impl CarV2 {
    pub fn new(header: CarHeaderV2, car_v1: v1::CarV1, index: Option<CarV2Index>) -> Self {
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


pub fn parse_v2_header(header: [u8; HEADER_LENGTH]) -> CarResult<CarHeaderV2> {
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

// TODO: Finish index parsing
pub fn read_v2_index<R: Read + Seek>(mut r: R, index_offset: u64) -> CarResult<Option<CarV2Index>> {
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

