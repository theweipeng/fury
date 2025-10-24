// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::{NaiveDate, NaiveDateTime};
use fory_core::fory::Fory;

// primitive_val
const BOOL_VAL: bool = true;
const I8_VAL: i8 = 42;
const I16_VAL: i16 = 43;
const I32_VAL: i32 = 44;
const I64_VAL: i64 = 45;
const F32_VAL: f32 = 46.0;
const F64_VAL: f64 = 47.0;
// string
const STR_LATIN1_VAL: &str = "Çüéâäàåçêëèïî";
// time
#[allow(deprecated)]
const TIMESTAMP_VAL: NaiveDateTime = NaiveDateTime::from_timestamp(100, 0);
#[allow(deprecated)]
const LOCAL_DATE_VAL: NaiveDate = NaiveDate::from_ymd(2021, 11, 23);

const BOOL_ARRAY: [bool; 1] = [true];
const INT8_ARRAY: [i8; 1] = [48];
const INT16_ARRAY: [i16; 1] = [49];
const INT32_ARRAY: [i32; 1] = [50];
const INT64_ARRAY: [i64; 1] = [51];
const FLOAT32_ARRAY: [f32; 1] = [52.0];
const FLOAT64_ARRAY: [f64; 1] = [53.0];

fn serialize_non_null(fory: &Fory) -> Vec<Vec<u8>> {
    vec![
        fory.serialize(&BOOL_VAL).unwrap(),
        fory.serialize(&I8_VAL).unwrap(),
        fory.serialize(&I16_VAL).unwrap(),
        fory.serialize(&I32_VAL).unwrap(),
        fory.serialize(&I64_VAL).unwrap(),
        fory.serialize(&F32_VAL).unwrap(),
        fory.serialize(&F64_VAL).unwrap(),
        fory.serialize(&STR_LATIN1_VAL.to_string()).unwrap(),
        fory.serialize(&LOCAL_DATE_VAL).unwrap(),
        fory.serialize(&TIMESTAMP_VAL).unwrap(),
        fory.serialize(&BOOL_ARRAY.to_vec()).unwrap(),
        fory.serialize(&INT8_ARRAY.to_vec()).unwrap(),
        fory.serialize(&INT16_ARRAY.to_vec()).unwrap(),
        fory.serialize(&INT32_ARRAY.to_vec()).unwrap(),
        fory.serialize(&INT64_ARRAY.to_vec()).unwrap(),
        fory.serialize(&FLOAT32_ARRAY.to_vec()).unwrap(),
        fory.serialize(&FLOAT64_ARRAY.to_vec()).unwrap(),
    ]
}

fn serialize_nullable(fory: &Fory) -> Vec<Vec<u8>> {
    vec![
        fory.serialize(&Some(BOOL_VAL)).unwrap(),
        fory.serialize(&Some(I8_VAL)).unwrap(),
        fory.serialize(&Some(I16_VAL)).unwrap(),
        fory.serialize(&Some(I32_VAL)).unwrap(),
        fory.serialize(&Some(I64_VAL)).unwrap(),
        fory.serialize(&Some(F32_VAL)).unwrap(),
        fory.serialize(&Some(F64_VAL)).unwrap(),
        fory.serialize(&Some(STR_LATIN1_VAL.to_string())).unwrap(),
        fory.serialize(&Some(LOCAL_DATE_VAL)).unwrap(),
        fory.serialize(&Some(TIMESTAMP_VAL)).unwrap(),
        fory.serialize(&Some(BOOL_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(INT8_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(INT16_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(INT32_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(INT64_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(FLOAT32_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Some(FLOAT64_ARRAY.to_vec())).unwrap(),
        fory.serialize(&Option::<bool>::None).unwrap(),
        fory.serialize(&Option::<i8>::None).unwrap(),
        fory.serialize(&Option::<i16>::None).unwrap(),
        fory.serialize(&Option::<i32>::None).unwrap(),
        fory.serialize(&Option::<i64>::None).unwrap(),
        fory.serialize(&Option::<f32>::None).unwrap(),
        fory.serialize(&Option::<f64>::None).unwrap(),
        fory.serialize(&Option::<String>::None).unwrap(),
        fory.serialize(&Option::<NaiveDate>::None).unwrap(),
        fory.serialize(&Option::<NaiveDateTime>::None).unwrap(),
        fory.serialize(&Option::<Vec<bool>>::None).unwrap(),
        fory.serialize(&Option::<Vec<i8>>::None).unwrap(),
        fory.serialize(&Option::<Vec<i16>>::None).unwrap(),
        fory.serialize(&Option::<Vec<i32>>::None).unwrap(),
        fory.serialize(&Option::<Vec<i64>>::None).unwrap(),
        fory.serialize(&Option::<Vec<f32>>::None).unwrap(),
        fory.serialize(&Option::<Vec<f64>>::None).unwrap(),
    ]
}

fn deserialize_non_null(fory: &Fory, mut bins: Vec<Vec<u8>>, auto_conv: bool) {
    bins.reverse();
    assert_eq!(
        BOOL_VAL,
        fory.deserialize::<bool>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        I8_VAL,
        fory.deserialize::<i8>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        I16_VAL,
        fory.deserialize::<i16>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        I32_VAL,
        fory.deserialize::<i32>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        I64_VAL,
        fory.deserialize::<i64>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        F32_VAL,
        fory.deserialize::<f32>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        F64_VAL,
        fory.deserialize::<f64>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        STR_LATIN1_VAL.to_string(),
        fory.deserialize::<String>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        LOCAL_DATE_VAL,
        fory.deserialize::<NaiveDate>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        TIMESTAMP_VAL,
        fory.deserialize::<NaiveDateTime>(bins.pop().unwrap().as_slice())
            .unwrap()
    );

    assert_eq!(
        BOOL_ARRAY.to_vec(),
        fory.deserialize::<Vec<bool>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        INT8_ARRAY.to_vec(),
        fory.deserialize::<Vec<i8>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        INT16_ARRAY.to_vec(),
        fory.deserialize::<Vec<i16>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        INT32_ARRAY.to_vec(),
        fory.deserialize::<Vec<i32>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        INT64_ARRAY.to_vec(),
        fory.deserialize::<Vec<i64>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        FLOAT32_ARRAY.to_vec(),
        fory.deserialize::<Vec<f32>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        FLOAT64_ARRAY.to_vec(),
        fory.deserialize::<Vec<f64>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    if auto_conv {
        assert_eq!(
            bool::default(),
            fory.deserialize::<bool>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            i8::default(),
            fory.deserialize::<i8>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            i16::default(),
            fory.deserialize::<i16>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            i32::default(),
            fory.deserialize::<i32>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            i64::default(),
            fory.deserialize::<i64>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            f32::default(),
            fory.deserialize::<f32>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            f64::default(),
            fory.deserialize::<f64>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            String::default(),
            fory.deserialize::<String>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            NaiveDate::default(),
            fory.deserialize::<NaiveDate>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            NaiveDateTime::default(),
            fory.deserialize::<NaiveDateTime>(bins.pop().unwrap().as_slice())
                .unwrap()
        );

        assert_eq!(
            Vec::<bool>::default(),
            fory.deserialize::<Vec<bool>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<i8>::default(),
            fory.deserialize::<Vec<i8>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<i16>::default(),
            fory.deserialize::<Vec<i16>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<i32>::default(),
            fory.deserialize::<Vec<i32>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<i64>::default(),
            fory.deserialize::<Vec<i64>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<f32>::default(),
            fory.deserialize::<Vec<f32>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            Vec::<f64>::default(),
            fory.deserialize::<Vec<f64>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
    }
}

fn deserialize_nullable(fory: &Fory, mut bins: Vec<Vec<u8>>, auto_conv: bool) {
    bins.reverse();
    assert_eq!(
        Some(BOOL_VAL),
        fory.deserialize::<Option<bool>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(I8_VAL),
        fory.deserialize::<Option<i8>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(I16_VAL),
        fory.deserialize::<Option<i16>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(I32_VAL),
        fory.deserialize::<Option<i32>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(I64_VAL),
        fory.deserialize::<Option<i64>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(F32_VAL),
        fory.deserialize::<Option<f32>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(F64_VAL),
        fory.deserialize::<Option<f64>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(STR_LATIN1_VAL.to_string()),
        fory.deserialize::<Option<String>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(LOCAL_DATE_VAL),
        fory.deserialize::<Option<NaiveDate>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(TIMESTAMP_VAL),
        fory.deserialize::<Option<NaiveDateTime>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );

    assert_eq!(
        Some(BOOL_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<bool>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(INT8_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<i8>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(INT16_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<i16>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(INT32_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<i32>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(INT64_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<i64>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(FLOAT32_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<f32>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    assert_eq!(
        Some(FLOAT64_ARRAY.to_vec()),
        fory.deserialize::<Option<Vec<f64>>>(bins.pop().unwrap().as_slice())
            .unwrap()
    );
    if !auto_conv {
        assert_eq!(
            None,
            fory.deserialize::<Option<bool>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<i8>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<i16>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<i32>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<i64>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<f32>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<f64>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<String>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<NaiveDate>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<NaiveDateTime>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );

        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<bool>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<i8>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<i16>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<i32>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<i64>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<f32>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
        assert_eq!(
            None,
            fory.deserialize::<Option<Vec<f64>>>(bins.pop().unwrap().as_slice())
                .unwrap()
        );
    }
}

// non-null <-> non-null
#[test]
fn basic() {
    let fory = Fory::default().compatible(true);
    // serialize
    let bins = serialize_non_null(&fory);
    // deserialize
    deserialize_non_null(&fory, bins, false);
}

// nullable <-> nullable
#[test]
fn basic_nullable() {
    let fory = Fory::default().compatible(true);
    // serialize
    let bins = serialize_nullable(&fory);
    // deserialize
    deserialize_nullable(&fory, bins, false);
}

// non-null -> nullable -> non-null
#[test]
fn auto_conv() {
    let fory = Fory::default().compatible(true);
    // serialize_non-null
    let bins = serialize_non_null(&fory);
    deserialize_nullable(&fory, bins, true);
    // serialize_nullable
    let bins = serialize_nullable(&fory);
    // deserialize_non-null
    deserialize_non_null(&fory, bins, true);
}
