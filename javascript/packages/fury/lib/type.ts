/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { BinaryWriter } from "./writer";
import type { BinaryReader } from "./reader";
import type FuryFunc from "./fury";

export type Fury = ReturnType<typeof FuryFunc>;
export type BinaryWriter = ReturnType<typeof BinaryWriter>
export type BinaryReader = ReturnType<typeof BinaryReader>


export enum InternalSerializerType{
    STRING = 13,
	ARRAY = 25,
	MAP = 30,
	BOOL = 1,
	UINT8 = 2,
	INT8 = 3,
	UINT16 = 4,
	INT16 = 5,
	UINT32 = 6,
	INT32 = 7,
	UINT64 = 8,
	INT64 = 9,
	FLOAT = 11,
	DOUBLE = 12,
	BINARY = 14,
	DATE = 16,
	TIMESTAMP = 18,
    FURY_TYPE_TAG = 256,
	FURY_SET = 257,
	FURY_PRIMITIVE_BOOL_ARRAY = 258,
	FURY_PRIMITIVE_SHORT_ARRAY = 259,
	FURY_PRIMITIVE_INT_ARRAY = 260,
	FURY_PRIMITIVE_LONG_ARRAY = 261,
	FURY_PRIMITIVE_FLOAT_ARRAY = 262,
	FURY_PRIMITIVE_DOUBLE_ARRAY = 263,
	FURY_STRING_ARRAY = 264,
	ANY = -1,
}


export enum ConfigFlags {
	isNullFlag = 1 << 0,
	isLittleEndianFlag = 2,
	isCrossLanguageFlag = 4,
	isOutOfBandFlag = 8,
}

export type SerializerRead<T = any> = (
) => T

export type SerializerWrite<T = any> = (
	v: T,
) => void

export type SerializerConfig = (
) => {
	reserve: number,
}


// read, write
export type Serializer<T = any, T2 = any> = {
	read: SerializerRead<T2>, 
	write: SerializerWrite<T>,
	readWithoutType?: SerializerRead<T2>, 
	writeWithoutType?: SerializerWrite<T>,
	config: SerializerConfig,
};



export enum RefFlags {
    NullFlag = -3,
	// RefFlag indicates that object is a not-null value.
	// We don't use another byte to indicate REF, so that we can save one byte.
	RefFlag = -2,
	// NotNullValueFlag indicates that the object is a non-null value.
	NotNullValueFlag = -1,
	// RefValueFlag indicates that the object is a referencable and first read.
	RefValueFlag = 0,
}

export const MaxInt32  = 2147483647;
export const LATIN1 = 0;
export const UTF8 = 1;

export interface Hps {
    isLatin1: (str: string) => boolean
    stringCopy: (str: string, dist: Uint8Array, offset: number) => void
}

export interface Config {
	hps?: Hps,
	refTracking?: boolean,
	useLatin1?: boolean,
	useSliceString?: boolean,
}