extern crate histogram;

use uuid::Uuid;

pub type HistogramPair = (histogram::Histogram, histogram::Histogram);

pub struct MantaObject {
    pub id: Uuid,
    pub name: String,
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub vnode: i64,
    pub content_length: i64,
    pub content_md5: String,
    pub content_type: String,
}
