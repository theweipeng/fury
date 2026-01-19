/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import Fory, {
  TypeInfo,
  InternalSerializerType,
  BinaryWriter,
} from "../packages/fory/index";
import { describe, expect, test } from "@jest/globals";
import * as fs from "node:fs";

const Byte = {
    MAX_VALUE: 127,
    MIN_VALUE: -128,
}

const Short = {
    MAX_VALUE: 32767,
    MIN_VALUE: -32768,
}

const Integer = {
    MAX_VALUE: 2147483647,
    MIN_VALUE: -2147483648,
}

const Long = {
    MAX_VALUE: BigInt("9223372036854775807"),
    MIN_VALUE: BigInt("-9223372036854775808"),
}

describe("bool", () => {
  const dataFile = process.env["DATA_FILE"];
  if (!dataFile) {
    return null;
  }
  function writeToFile(buffer: Buffer) {
    fs.writeFileSync(dataFile!, buffer);
  }
  const content = fs.readFileSync(dataFile);
  
  test("test_buffer", () => {
    const buffer = new BinaryWriter();
    buffer.reserve(32);
    buffer.bool(true);
    buffer.uint8(Byte.MAX_VALUE);
    buffer.int16(Short.MAX_VALUE);
    buffer.int32(Integer.MAX_VALUE);
    buffer.int64(Long.MAX_VALUE);
    buffer.float32(-1.1);
    buffer.float64(-1.1);
    buffer.varUInt32(100);
    const bytes = ['a'.charCodeAt(0), 'b'.charCodeAt(0)];
    buffer.int32(bytes.length);
    buffer.buffer(new Uint8Array(bytes));
    writeToFile(buffer.dump() as Buffer);
  });
  test("test_buffer_var", () => {
    // todo
  });
  test("test_murmurhash3", () => {
    // todo
  });
  test("test_string_serializer", () => {
    // todo
  });
  test("test_cross_language_serializer", () => {
    // todo
  });
  test("test_simple_struct", () => {
    // todo
  });
  test("test_named_simple_struct", () => {
    // todo
  });
  test("test_list", () => {
    // todo
  });
  test("test_map", () => {
    // todo
  });
  test("test_integer", () => {
    // todo
  });
  test("test_item", () => {
    // todo
  });
  test("test_color", () => {
    // todo
  });
  test("test_struct_with_list", () => {
    // todo
  });
  test("test_struct_with_map", () => {
    // todo
  });
  test("test_skip_id_custom", () => {
    // todo
  });
  test("test_skip_name_custom", () => {
    // todo
  });
  test("test_consistent_named", () => {
    // todo
  });
  test("test_struct_version_check", () => {
    // todo
  });
  test("test_polymorphic_list", () => {
    // todo
  });
  test("test_polymorphic_map", () => {
    // todo
  });
  test("test_one_string_field_schema", () => {
    // todo
  });
  test("test_one_string_field_compatible", () => {
    // todo
  });
  test("test_two_string_field_compatible", () => {
    // todo
  });
  test("test_schema_evolution_compatible", () => {
    // todo
  });
  test("test_one_enum_field_schema", () => {
    // todo
  });
  test("test_one_enum_field_compatible", () => {
    // todo
  });
  test("test_two_enum_field_compatible", () => {
    // todo
  });
  test("test_enum_schema_evolution_compatible", () => {
    // todo
  });
  test("test_nullable_field_schema_consistent_not_null", () => {
    // todo
  });
  test("test_nullable_field_schema_consistent_null", () => {
    // todo
  });
  test("test_nullable_field_compatible_not_null", () => {
    // todo
  });
  test("test_nullable_field_compatible_null", () => {
    // todo
  });
  test("test_ref_schema_consistent", () => {
    // todo
  });
  test("test_ref_compatible", () => {
    // todo
  });
  test("test_circular_ref_schema_consistent", () => {
    // todo
  });
  test("test_circular_ref_compatible", () => {
    // todo
  });
  test("test_unsigned_schema_consistent_simple", () => {
    // todo
  });
  test("test_unsigned_schema_consistent", () => {
    // todo
  });
  test("test_unsigned_schema_compatible", () => {
    // todo
  });
});
