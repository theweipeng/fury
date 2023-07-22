use chrono::{NaiveDate, NaiveDateTime};
use fury::{from_buffer, to_buffer};
use fury_derive::{Deserialize, FuryMeta, Serialize};
use std::collections::HashMap;

#[test]
fn complex_struct() {
    #[derive(FuryMeta, Serialize, Deserialize, Debug, PartialEq)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(FuryMeta, Serialize, Deserialize, Debug, PartialEq)]
    #[tag("example.foo")]
    struct Person {
        c1: Vec<u8>,  // binary
        c2: Vec<i16>, // primitive array
        animal: Vec<Animal>,
        c3: Vec<Vec<u8>>,
        name: String,
        c4: HashMap<String, String>,
        age: u16,
        op: Option<String>,
        op2: Option<String>,
        date: NaiveDate,
        time: NaiveDateTime,
    }
    let person: Person = Person {
        c1: vec![1, 2, 3],
        c2: vec![5, 6, 7],
        c3: vec![vec![1, 2], vec![1, 3]],
        animal: vec![Animal {
            category: "Dog".to_string(),
        }],
        c4: HashMap::from([
            ("hello1".to_string(), "hello2".to_string()),
            ("hello2".to_string(), "hello3".to_string()),
        ]),
        age: 12,
        name: "helo".to_string(),
        op: Some("option".to_string()),
        op2: None,
        date: NaiveDate::from_ymd_opt(2025, 12, 12).unwrap(),
        time: NaiveDateTime::from_timestamp_opt(1689912359, 0).unwrap(),
    };

    let bin: Vec<u8> = to_buffer(&person);
    let obj: Person = from_buffer(&bin).expect("should some");
    assert_eq!(person, obj);
}



#[test]
fn decode_py_struct() {
    #[derive(FuryMeta, Serialize, Deserialize, Debug, PartialEq)]
    #[tag("example.foo2")]
    struct Animal {
        category: String,
    }

    #[derive(FuryMeta, Serialize, Deserialize, Debug, PartialEq)]
    #[tag("example.ComplexObject")]
    struct Person {
        f1: String,
        f2: HashMap<String, i8>,
        f3: i8,
        f4: i16,
        f5: i32,
        f6: i64,
        f7: f32,
        f8: f64,
        f9: Vec<i16>,
        f10: HashMap<i32, f64>
    }

    let bin = [
        6,2,173,0,0,0,0,0,0,0,0,0,1,0,81,159,160,124,69,240,2,120,21,0,101,120,97,109,112,108,101,46,67,111,109,112,108,101,120,79,98,106,101,99,116,71,168,32,21,0,13,0,3,115,116,114,0,30,0,2,0,7,0,1,0,0,0,0,12,0,85,85,85,85,85,85,213,63,0,7,0,100,0,0,0,0,12,0,146,36,73,146,36,73,210,63,0,30,0,2,0,13,0,2,107,49,0,3,0,255,0,13,0,2,107,50,0,3,0,2,0,3,0,127,0,5,0,255,127,0,7,0,255,255,255,127,0,9,0,255,255,255,255,255,255,255,127,0,11,0,0,0,0,63,0,12,0,85,85,85,85,85,85,229,63,0,25,0,2,254,3,254,11,6,2,72,0,0,0,0,0,0,0,0,0,1,0,81,159,160,124,69,240,2,120,21,0,101,120,97,109,112,108,101,46,67,111,109,112,108,101,120,79,98,106,101,99,116,71,168,32,21,253,253,253,0,3,0,0,254,1,254,1,254,1,0,11,0,171,170,170,62,254,1,253,

    ];

    let obj: Person = from_buffer(&bin).expect("should some");
    print!("{:?}", obj);
}
