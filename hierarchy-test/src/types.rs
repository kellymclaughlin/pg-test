extern crate histogram;

use std::collections::HashMap;
use std::iter;

use rand::distributions::Alphanumeric;
use rand::{Rng, ThreadRng};
use uuid::Uuid;

use common;

pub type HistogramPair = (histogram::Histogram, histogram::Histogram);

pub struct MantaObject {
    pub id             : Uuid,
    pub name           : String,
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub vnode          : i64,
    pub content_length : i64,
    pub content_md5    : String,
    pub content_type   : String,
    pub headers        : HashMap<String, Option<String>>,
    pub sharks         : HashMap<String, Option<String>>
}

impl MantaObject {
    pub fn new(rng: &mut ThreadRng) -> MantaObject {
        let name: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(10)
            .collect();

        MantaObject {
            name: name,
            id: Uuid::new_v4(),
            bucket_id: Uuid::new_v4(),
            vnode: 1000,
            owner: Uuid::new_v4(),
            content_length: 1024,
            content_md5: String::from("deadbeef"),
            content_type: String::from("text/plain"),
            headers: common::headers(),
            sharks: common::sharks()
        }
    }
}
