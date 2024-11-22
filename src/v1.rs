use std::io::{Cursor, Read};
use cid::Cid;
use libipld::{Block, DefaultParams, Ipld};
use crate::{CarError, CarResult};
use unsigned_varint::io::read_u64 as varint_read_u64;

/// An IPLD Content Archive Version 1
#[derive(Debug, Clone)]
pub struct CarV1 {
    pub header: CarHeaderV1,
    pub blocks: Vec<Block<DefaultParams>>,
}

/// An IPLD Content Archive Header Version 1
#[derive(Debug, Clone)]
pub struct CarHeaderV1 {
    pub roots: Vec<Cid>,
}

impl CarV1 {
    pub fn new(header: CarHeaderV1, blocks: Vec<Block<DefaultParams>>) -> Self {
        Self { header, blocks }
    }
}

pub fn parse_v1_header(header_map: Ipld) -> CarResult<CarHeaderV1> {
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

