/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.sovereign.common.dumbstruct;

import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;

/**
 * This basically functions as a pointer (immutable) into a DataBuffer for dumbstruct access
 *
 * @author cschanck
 */
public class Accessor {
  private final DataBuffer db;
  private final int offset;

  /**
   * Instantiates a new Accessor.
   *
   * @param db the db
   */
  public Accessor(DataBuffer db) {
    this(db, 0);
  }

  /**
   * Instantiates a new Accessor.
   *
   * @param db     the db
   * @param offset the offset
   */
  public Accessor(DataBuffer db, int offset) {
    this.db = db;
    this.offset = offset;
  }

  /**
   * Gets data buffer.
   *
   * @return the data buffer
   */
  public final DataBuffer getDataBuffer() {
    return db;
  }

  /**
   * Gets offset.
   *
   * @return the offset
   */
  public final int getOffset() {
    return offset;
  }

  /**
   * Ceate a new accessor with the same data buffer and a new offset.
   *
   * @param offset new offset.
   * @return New accessor.
   */
  public Accessor repoint(int offset) {
    return new Accessor(getDataBuffer(), offset);
  }

  /**
   * Create a new accessor, incrementing the offset by the specified number of bytes.
   *
   * @param bytes bytes to add
   * @return new accessor
   */
  public Accessor increment(int bytes) {
    return new Accessor(getDataBuffer(), offset + bytes);
  }

  /**
   * Create a new accessor, decrementing the offset by the specified number of bytes.
   *
   * @param bytes bytes to subtract
   * @return new accessor
   */
  public Accessor decrement(int bytes) {
    return increment(-bytes);
  }

  /**
   * Create a new accessor, incrementing the offset by the size of a single field of the
   * specified struct.
   *
   * @param field field to use
   * @return new accessor
   */
  public Accessor increment(StructField field) {
    return increment(field, 1);
  }

  /**
   * Create a new accessor, decrementing the offset by the size of a single field of the
   * specified struct.
   *
   * @param field field to use
   * @return new accessor
   */
  public Accessor decrement(StructField field) {
    return decrement(field, 1);
  }

  /**
   * Create a new accessor, incrementing the offset by the size of a single field of the
   * specified struct * number of entries.
   *
   * @param field field to use
   * @param many  number of fields to use
   * @return new accessor
   */
  public Accessor increment(StructField field, int many) {
    return increment(many * field.getSingleFieldSize());
  }

  /**
   * Create a new accessor, decrementing the offset by the size of a single field of the
   * specified struct * number of entries.
   *
   * @param field field to use
   * @param many  number of fields to use
   * @return new accessor
   */
  public Accessor decrement(StructField field, int many) {
    return decrement(many * field.getSingleFieldSize());
  }

}
