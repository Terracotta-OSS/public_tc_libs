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
package com.terracottatech.sovereign.common.dumbstruct.fields.composite;

import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.fields.AbstractStructField;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int8Field;

/**
 * @author cschanck
 */
public abstract class AbstractSizedByteArrayField extends AbstractStructField implements SizedByteArrayField {

  private final ByteArrayField array;

  /**
   * Instantiates a new Small byte array field.
   *
   * @param offsetWithinParent the offset within parent
   * @param size the size
   */
  public AbstractSizedByteArrayField(String id, int offsetWithinParent, int size, int lengthSize) {
    super(id, offsetWithinParent, 1, size + lengthSize);
    this.array = new ByteArrayField(getOffsetWithinStruct() + lengthSize, size);
  }

  protected abstract void setLength(Accessor a, int length);

  @Override
  public byte[] getBytes(Accessor a) {
    int many = getLength(a);
    byte[] ret = new byte[many];
    array.get(a, 0, ret, 0, ret.length);
    return ret;
  }

  /**
   * Gets bytes.
   *
   * @param a the a
   * @param dest the dest
   * @param offset the offset
   */
  @Override
  public void getBytes(Accessor a, byte[] dest, int offset) {
    int many = getLength(a);
    if (many > dest.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    array.get(a, 0, dest, offset, many);
  }

  @Override
  public void putBytes(Accessor a, byte[] barr) {
    if (barr.length > array.getAllocatedSize()) {
      throw new IndexOutOfBoundsException();
    }
    setLength(a, barr.length);
    array.put(a, 0, barr, 0, barr.length);
  }

  @Override
  public void putBytes(Accessor a, byte[] barr, int srcOffset, int length) {
    if (length > array.getAllocatedSize()) {
      throw new IndexOutOfBoundsException();
    }
    setLength(a, length);
    array.put(a, 0, barr, srcOffset, length);
  }

  /**
   * The type Small byte array field.
   *
   * @author cschanck
   */
  public static class SmallByteArrayField extends AbstractSizedByteArrayField {

    private final Int8Field len;

    /**
     * Instantiates a new Small byte array field.
     *
     * @param offsetWithinParent the offset within parent
     * @param size the size
     */
    public SmallByteArrayField(int offsetWithinParent, int size) {
      super("F bytes8", offsetWithinParent, size, 1);
      if (size > Byte.MAX_VALUE) {
        throw new IllegalArgumentException();
      }
      this.len = new Int8Field(getOffsetWithinStruct(), 1);
    }

    /**
     * Gets length.
     *
     * @param a the a
     * @return the length
     */
    @Override
    public int getLength(Accessor a) {
      return len.get(a);
    }

    @Override
    protected void setLength(Accessor a, int length) {
      len.put(a, (byte) length);
    }
  }

  /**
   * The type Medium byte array field.
   *
   * @author cschanck
   */
  public static class MediumByteArrayField extends AbstractSizedByteArrayField {

    private final Int16Field len;

    /**
     * Instantiates a new Medium byte array field.
     *
     * @param offsetWithinParent the offset within parent
     * @param size the size
     */
    public MediumByteArrayField(int offsetWithinParent, int size) {
      super("F bytes16", offsetWithinParent, size, 2);
      if (size > Short.MAX_VALUE) {
        throw new IllegalArgumentException();
      }
      this.len = new Int16Field(getOffsetWithinStruct(), 1);
    }

    /**
     * Gets length.
     *
     * @param a the a
     * @return the length
     */
    @Override
    public int getLength(Accessor a) {
      return len.get(a);
    }

    @Override
    protected void setLength(Accessor a, int length) {
      len.put(a, (short) length);
    }
  }

  /**
   * The type Large byte array field.
   *
   * @author cschanck
   */
  public static class LargeByteArrayField extends AbstractSizedByteArrayField {

    private final Int32Field len;

    /**
     * Instantiates a new Large byte array field.
     *
     * @param offsetWithinParent the offset within parent
     * @param size the size
     */
    public LargeByteArrayField(int offsetWithinParent, int size) {
      super("F bytes32", offsetWithinParent, size, 4);
      this.len = new Int32Field(getOffsetWithinStruct(), 1);
    }

    /**
     * Gets length.
     *
     * @param a the a
     * @return the length
     */
    @Override
    public int getLength(Accessor a) {
      return len.get(a);
    }

    @Override
    protected void setLength(Accessor a, int length) {
      len.put(a, length);
    }
  }
}
