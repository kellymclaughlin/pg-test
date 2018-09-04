extern crate histogram;

use std::collections::HashMap;

use uuid::Uuid;

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
