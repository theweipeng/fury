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

import { TypeId, MaxInt32, Mode, RefFlags } from "../type";
import { Scope } from "./scope";
import { CodecBuilder } from "./builder";
import { StructTypeInfo, TypeInfo } from "../typeInfo";
import { CodegenRegistry } from "./router";
import { BaseSerializerGenerator, SerializerGenerator } from "./serializer";
import { fromString } from "../platformBuffer";
import { TypeMeta } from "../meta/TypeMeta";

function computeFieldHash(hash: number, id: number): number {
  let newHash = (hash) * 31 + (id);
  while (newHash >= MaxInt32) {
    newHash = Math.floor(newHash / 7);
  }
  return newHash;
}

const computeStringHash = (str: string) => {
  const bytes = fromString(str);
  let hash = 17;
  bytes.forEach((b) => {
    hash = hash * 31 + b;
    while (hash >= MaxInt32) {
      hash = Math.floor(hash / 7);
    }
  });
  return hash;
};

const computeStructHash = (typeInfo: TypeInfo) => {
  let hash = 17;
  for (const [, value] of Object.entries((<StructTypeInfo>typeInfo).options.props!).sort()) {
    let id = value.typeId;
    if (TypeId.isNamedType(value.typeId!)) {
      id = computeStringHash((<StructTypeInfo>value).namespace! + (<StructTypeInfo>value).typeName!);
    }
    hash = computeFieldHash(hash, id || 0);
  }
  return hash;
};

const sortProps = (typeInfo: StructTypeInfo) => {
  const names = TypeMeta.fromTypeInfo(typeInfo).getFieldInfo();
  const props = typeInfo.options.props;
  return names.map(x => {
    return {
      key: x.fieldName,
      typeInfo: props![x.fieldName]
    };
  });
}

enum RefMode {
  /** No ref tracking, field is non-nullable. Write data directly. */
  NONE,

  /** Field is nullable but no ref tracking. Write null/not-null flag then data. */
  NULL_ONLY,

  /** Ref tracking enabled (implies nullable). Write ref flags and handle shared references. */
  TRACKING


}

function toRefMode(trackingRef: boolean, nullable: boolean) {
  if (trackingRef) {
    return RefMode.TRACKING;
  } else if (nullable) {
    return RefMode.NULL_ONLY;
  } else {
    return RefMode.NONE;
  }
}

class StructSerializerGenerator extends BaseSerializerGenerator {
  typeInfo: StructTypeInfo;
  sortedProps: { key: string, typeInfo: TypeInfo }[];

  constructor(typeInfo: TypeInfo, builder: CodecBuilder, scope: Scope) {
    super(typeInfo, builder, scope);
    this.typeInfo = <StructTypeInfo>typeInfo;
    this.sortedProps = sortProps(this.typeInfo);
  }

  xreadField(fieldName: string, fieldTypeInfo: TypeInfo, assignStmt: (expr: string) => string, innerGenerator: SerializerGenerator) {
    const { nullable = false, trackingRef = false } = this.typeInfo.options.fieldInfo?.[fieldName] || {};
    const refMode = toRefMode(trackingRef, nullable);
    let stmt = '';
    // polymorphic type
    if (fieldTypeInfo.isMonomorphic()) {
      if (refMode == RefMode.TRACKING || refMode === RefMode.NULL_ONLY) {
        stmt = `
            ${innerGenerator.xreadRef(assignStmt)}
        `
        } else {
          stmt = innerGenerator.xread(assignStmt, "false");
        }
    } else {
        if (refMode == RefMode.TRACKING || refMode === RefMode.NULL_ONLY) {
           stmt = `${innerGenerator.xreadRef(assignStmt)}`
        } else {
          stmt = innerGenerator.xreadNoRef(assignStmt, "false");
        }
    }
    return stmt;
  }

  xwriteField(fieldName:string, fieldTypeInfo: TypeInfo, fieldAccessor: string, innerGenerator: SerializerGenerator) {
    const { nullable = false, trackingRef = false } = this.typeInfo.options.fieldInfo?.[fieldName] || {};
    const refMode = toRefMode(trackingRef, nullable);
    let stmt = '';
    // polymorphic type
    if (fieldTypeInfo.isMonomorphic()) {
      if (refMode == RefMode.TRACKING) {
        const noneedWrite = this.scope.uniqueName("noneedWrite");
        stmt = `
            let ${noneedWrite} = false;
            ${innerGenerator.writeRefOrNull(expr => `${noneedWrite} = ${expr}`, fieldAccessor)}
            if (!${noneedWrite}) {
              ${innerGenerator.xwrite(fieldAccessor)}
            }
        `
        } else if (refMode == RefMode.NULL_ONLY) {
          stmt = `
            if (${fieldAccessor} === null || ${fieldAccessor} === undefined) {
              ${this.builder.writer.int8(RefFlags.NullFlag)}
            } else {
              ${this.builder.writer.int8(RefFlags.NotNullValueFlag)}
              ${innerGenerator.xwrite(fieldAccessor)}
            }
          `;
        } else {
          stmt = `
            if (${fieldAccessor} === null || ${fieldAccessor} === undefined) {
              throw new Error('Field ${CodecBuilder.safeString(fieldName)} is not nullable');
            } else {
              ${innerGenerator.xwrite(fieldAccessor)}
            }
          `
        }
    } else {
        if (refMode == RefMode.TRACKING) {
           stmt = `${innerGenerator.xwriteRef(fieldAccessor)}`
        } else if (refMode == RefMode.NULL_ONLY) {
          stmt = `
            if (${fieldAccessor} === null || ${fieldAccessor} === undefined) {
              ${this.builder.writer.int8(RefFlags.NullFlag)}
            } else {
              ${this.builder.writer.int8(RefFlags.NotNullValueFlag)}
              ${innerGenerator.xwriteNoRef(fieldAccessor)}
            }
          `
        } else {
          stmt = `
            if (${fieldAccessor} === null || ${fieldAccessor} === undefined) {
              throw new Error('Field ${CodecBuilder.safeString(fieldName)} is not nullable');
            } else {
              ${innerGenerator.xwriteNoRef(fieldAccessor)}
            }
          `
        }
    }
    return stmt;
  }

  xwrite(accessor: string): string {
    return `
      ${this.sortedProps.map(({ key, typeInfo }) => {
      const InnerGeneratorClass = CodegenRegistry.get(typeInfo.typeId);
      if (!InnerGeneratorClass) {
        throw new Error(`${typeInfo.typeId} generator not exists`);
      }
      const innerGenerator = new InnerGeneratorClass(typeInfo, this.builder, this.scope);

      const fieldAccessor = `${accessor}${CodecBuilder.safePropAccessor(key)}`;
      return this.xwriteField(key, typeInfo, fieldAccessor, innerGenerator.xwriteEmbed());
    }).join(";\n")}
    `;
  }

  xread(accessor: (expr: string) => string, refState: string): string {
    const result = this.scope.uniqueName("result");
    return `
      ${this.typeInfo.options.withConstructor
        ? `
          const ${result} = new ${this.builder.getOptions("constructor")}();
        `
        : `
          const ${result} = {
            ${this.sortedProps.map(({ key }) => {
          return `${CodecBuilder.safePropName(key)}: null`;
        }).join(",\n")}
          };
        `
      }
      ${this.maybeReference(result, refState)}
      ${this.sortedProps.map(({ key, typeInfo }) => {
        const InnerGeneratorClass = CodegenRegistry.get(typeInfo.typeId);
        if (!InnerGeneratorClass) {
          throw new Error(`${typeInfo.typeId} generator not exists`);
        }
        const innerGenerator = new InnerGeneratorClass(typeInfo, this.builder, this.scope);
        return this.xreadField(key, typeInfo, expr => `${result}${CodecBuilder.safePropAccessor(key)} = ${expr}`, innerGenerator.xreadEmbed());
      }).join(";\n")}
      ${accessor(result)}
    `;
  }

  readClassInfo(): string {
    const typeMeta = this.scope.uniqueName("typeMeta");
    let namesStmt = '';
    if (!this.builder.fory.isCompatible() && TypeId.isNamedType(this.getTypeId())) {
      namesStmt = `
        ${
          this.builder.metaStringResolver.readNamespace(this.builder.reader.ownName())
        };
        ${
          this.builder.metaStringResolver.readTypeName(this.builder.reader.ownName())
        };
      `
    }
    let typeMetaStmt = '';
    if (this.builder.fory.isCompatible()) {
      typeMetaStmt = `
      const ${typeMeta} = ${this.builder.typeMetaResolver.readTypeMeta(this.builder.reader.ownName())};
      if (getHash() !== ${typeMeta}.getHash()) {
        ${this.builder.typeMetaResolver.genSerializerByTypeMetaRuntime(typeMeta)}
      }
      `
    }
    return `
      ${
        this.builder.reader.readVarUint32Small7()
      };
      ${
        namesStmt
      }
      ${
        typeMetaStmt
      }
    `
  }

  xreadEmbed() {
    return new Proxy({}, {
        get: (target, prop: string) => {
          return (accessor: (expr: string) => string, ...args: string[]) => {
            const name = this.scope.declare(
              "tag_ser",
              TypeId.isNamedType(this.typeInfo.typeId)
                ? this.builder.classResolver.getSerializerByName(CodecBuilder.replaceBackslashAndQuote(this.typeInfo.named!))
                : this.builder.classResolver.getSerializerById(this.typeInfo.typeId)
            );
            return accessor(`${name}.${prop}(${args.join(',')})`);
          }
        }
    });
  }

  xwriteEmbed() {
    return new Proxy({}, {
        get: (target, prop: string) => {
          return (accessor: string) => {
            const name = this.scope.declare(
              "tag_ser",
              TypeId.isNamedType(this.typeInfo.typeId)
                ? this.builder.classResolver.getSerializerByName(CodecBuilder.replaceBackslashAndQuote(this.typeInfo.named!))
                : this.builder.classResolver.getSerializerById(this.typeInfo.typeId)
            );
            return `${name}.${prop}(${accessor})`;
          }
        }
    });
  }

  writeClassInfo(): string {
    const internalTypeId = this.getInternalTypeId();
    let typeMeta = '';
    switch (internalTypeId) {
      case TypeId.NAMED_STRUCT:
      case TypeId.NAMED_COMPATIBLE_STRUCT:
        if (this.builder.fory.config.mode !== Mode.Compatible) {
          const typeInfo = this.typeInfo.castToStruct();
          const nsBytes = this.scope.declare("nsBytes", this.builder.metaStringResolver.encodeNamespace(CodecBuilder.replaceBackslashAndQuote(typeInfo.namespace)));
          const typeNameBytes = this.scope.declare("typeNameBytes", this.builder.metaStringResolver.encodeTypeName(CodecBuilder.replaceBackslashAndQuote(typeInfo.typeName)));
          typeMeta = `
            ${this.builder.metaStringResolver.writeBytes(this.builder.writer.ownName(), nsBytes)}
            ${this.builder.metaStringResolver.writeBytes(this.builder.writer.ownName(), typeNameBytes)}
          `;

        } else {
          const bytes = this.scope.declare("typeInfoBytes", `new Uint8Array([${TypeMeta.fromTypeInfo(<StructTypeInfo>this.typeInfo).toBytes().join(",")}])`);
          typeMeta = this.builder.typeMetaResolver.writeTypeMeta(this.builder.getTypeInfo(), this.builder.writer.ownName(), bytes);
        }
        break;
      case TypeId.COMPATIBLE_STRUCT:
        const bytes = this.scope.declare("typeInfoBytes", `new Uint8Array([${TypeMeta.fromTypeInfo(<StructTypeInfo>this.typeInfo).toBytes().join(",")}])`);
        typeMeta = this.builder.typeMetaResolver.writeTypeMeta(this.builder.getTypeInfo(), this.builder.writer.ownName(), bytes);
        break;
      default:
        break;
    }
    return ` 
      ${this.builder.writer.writeVarUint32Small7(this.getTypeId())};
      ${typeMeta}
    `
  }

  getFixedSize(): number {
    const typeInfo = <StructTypeInfo>this.typeInfo;
    const options = typeInfo.options;
    let fixedSize = 8;
    if (options.props) {
      Object.values(options.props).forEach((x) => {
        const propGenerator = new (CodegenRegistry.get(x.typeId)!)(x, this.builder, this.scope);
        fixedSize += propGenerator.getFixedSize();
      });
    } else {
      fixedSize += this.builder.fory.classResolver.getSerializerByName(typeInfo.named!)!.fixedSize;
    }
    return fixedSize;
  }

  getHash(): string {
    return TypeMeta.fromTypeInfo(this.typeInfo).getHash().toString();
  }
}

CodegenRegistry.register(TypeId.STRUCT, StructSerializerGenerator);
CodegenRegistry.register(TypeId.NAMED_STRUCT, StructSerializerGenerator);
CodegenRegistry.register(TypeId.COMPATIBLE_STRUCT, StructSerializerGenerator);
CodegenRegistry.register(TypeId.NAMED_COMPATIBLE_STRUCT, StructSerializerGenerator);
