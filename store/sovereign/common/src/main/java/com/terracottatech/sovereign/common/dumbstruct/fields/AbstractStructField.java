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
package com.terracottatech.sovereign.common.dumbstruct.fields;

import com.terracottatech.sovereign.common.dumbstruct.Accessor;

/**
 * @author cschanck
 */
public abstract class AbstractStructField implements StructField {
  private final int offsetWithinParent;
  private final int allocatedSize;
  private final int numberOfFields;
  private final int singleFieldSize;
  private final String id;

  /**
   * Instantiates a new Abstract struct field.
   *
   * @param id
   * @param offsetWithinParent the offsetWithinParent
   * @param many               the number of these fields to be allocated
   * @param size               the size of an individual field.
   *
   */
  public AbstractStructField(String id, int offsetWithinParent, int many, int size) {
    this.id = id;
    this.offsetWithinParent = offsetWithinParent;
    this.numberOfFields = many;
    this.singleFieldSize = size;
    this.allocatedSize = size * many;
  }

  /**
   * Gets offsetWithinParent.
   *
   * @return the offsetWithinParent
   */
  public int getOffsetWithinStruct() {
    return offsetWithinParent;
  }

  /**
   * Gets allocated size.
   *
   * @return the allocated size
   */
  public int getAllocatedSize() {
    return allocatedSize;
  }

  /**
   * Gets number of fields.
   *
   * @return the number of fields
   */
  public int getAllocationCount() {
    return numberOfFields;
  }

  /**
   * Gets single field size.
   *
   * @return the single field size
   */
  public int getSingleFieldSize() {
    return singleFieldSize;
  }

  public void move(Accessor a, int fromIndex, int toIndex, int many) {
    a.getDataBuffer().copyWithin(address(a, fromIndex), address(a, toIndex), deltaFor(many));
  }

  public Int8Field asInt8Array() {
    return new Int8Field(getOffsetWithinStruct(), getAllocatedSize());
  }

  @Override
  public String toString() {
    return id + " @" + getOffsetWithinStruct() + " #" + getAllocationCount() + "*" + getSingleFieldSize() + " (" + getAllocatedSize() + ")";
  }

  @Override
  public int deltaFor(int index) {
    return index * getSingleFieldSize();
  }

  protected int address(Accessor a) {
    return address(a, 0);
  }

  protected int address(Accessor a, int index) {
    return a.getOffset() + getOffsetWithinStruct() + deltaFor(index);
  }
}
