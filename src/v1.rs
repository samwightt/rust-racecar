use std::io::{Cursor, Read, Seek};
use libipld::{Block, DefaultParams, Ipld, cid::Cid, prelude::Codec};
use libipld::cbor::DagCborCodec;
use crate::{CarError, CarResult};
use unsigned_varint::io::read_u64 as varint_read_u64;

/// An IPLD Content Archive Version 1
#[derive(Debug, Clone)]
pub struct CarV1 {
    pub header: CarHeaderV1,
    pub blocks: Vec<Block<DefaultParams>>,
}

impl CarV1 {
    pub fn new(header: CarHeaderV1, blocks: Vec<Block<DefaultParams>>) -> Self {
        Self { header, blocks }
    }

    pub fn from_reader<R: Read>(mut r: R) -> CarResult<Self> {
        let header = CarHeaderV1::from_reader(&mut r)?;

        Ok(Self { header, blocks: read_car_v1_data(r)? })
    }
}

pub fn read_car_v1_data<R: Read>(mut r: R) -> CarResult<Vec<Block<DefaultParams>>> {
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


/// An IPLD Content Archive Header Version 1
#[derive(Debug, Clone)]
pub struct CarHeaderV1 {
    pub roots: Vec<Cid>,
}

impl CarHeaderV1 {
    fn from_reader<R: Read>(r: &mut R) -> CarResult<Self> {
        let header_length = varint_read_u64(&mut *r)?;

        let mut header_buf = vec![0; header_length as usize];
        r.read_exact(&mut header_buf)?;

        let header_map: Ipld = DagCborCodec.decode(&header_buf)?;
        let header = Self::from_ipld(header_map)?;

        Ok(header)
    }

    fn from_ipld(header_map: Ipld) -> CarResult<Self> {
        let version = header_map.get("version").map_err(|_| CarError::InvalidFormat)?;
        let roots_list = header_map.get("roots").map_err(|_| CarError::InvalidFormat)?;
        match (version, roots_list) {
            (Ipld::Integer(1), Ipld::List(cids)) => {
                let roots: Result<Vec<_>, _> =
                    cids.iter().map(|ipld| match ipld {
                        Ipld::Link(link) => Ok(link.clone()),
                        _ => Err(CarError::InvalidFormat),
                    }).collect();

                Ok(CarHeaderV1 { roots: roots? })
            }
            _ => Err(CarError::InvalidFormat),
        }
    }
}

