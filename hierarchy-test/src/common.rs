use std::collections::HashMap;

use histogram::Histogram;


pub fn sharks() -> HashMap<String, Option<String>> {
    let mut headers = HashMap::new();
    HashMap::insert(&mut headers,
                    String::from("us-east-1"),
                    Some(String::from("1.stor.us-east.joyent.com")));
    HashMap::insert(&mut headers, String::from("us-east-2"),
                    Some(String::from("3.stor.us-east.joyent.com'")));
    headers
}

pub fn headers() -> HashMap<String, Option<String>> {
    let mut headers = HashMap::new();
    HashMap::insert(&mut headers,
                    String::from("m-custom-header-1"),
                    Some(String::from("header-value1")));
    HashMap::insert(&mut headers, String::from("m-custom-header-2"),
                    Some(String::from("header-value2")));
    headers
}

pub fn print_results(read: &Histogram, write: &Histogram) -> () {
    println!(
        "Read Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        read.percentile(50.0).unwrap(),
        read.percentile(90.0).unwrap(),
        read.percentile(99.0).unwrap(),
        read.percentile(99.9).unwrap(),
    );

    println!(
        "Write Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        write.percentile(50.0).unwrap(),
        write.percentile(90.0).unwrap(),
        write.percentile(99.0).unwrap(),
        write.percentile(99.9).unwrap(),
    );
}
