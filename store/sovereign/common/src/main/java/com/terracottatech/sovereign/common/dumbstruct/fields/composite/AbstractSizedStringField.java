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
public abstract class AbstractSizedStringField extends AbstractStructField implements SizedStringField {

  private final StringField string;

  /**
   * Instantiates a new Abstract struct field.
   *
   * @param offsetWithinParent the offsetWithinParent
   * @param size               the size
   */
  public AbstractSizedStringField(String id, int offsetWithinParent, int size, int lengthSize) {
    super(id, offsetWithinParent, 1, size * 2 + lengthSize);
    this.string = new StringField(getOffsetWithinStruct() + lengthSize, size);
  }

  @Override
  public abstract int getLength(Accessor a);

  protected abstract void setLength(Accessor a, int length);

  @Override
  public String getString(Accessor a) {
    return string.getString(a, getLength(a));
  }

  @Override
  public void putString(Accessor a, CharSequence src) {
    if (src.length() > string.getAllocationCount()) {
      throw new IndexOutOfBoundsException();
    }
    setLength(a, src.length());
    string.putString(a, src);
  }

  /**
   * The type Small string field.
   *
   * @author cschanck
   */
  public static class SmallStringField extends AbstractSizedStringField {

    private final Int8Field len;

    /**
     * Instantiates a new Abstract struct field.
     *
     * @param offsetWithinParent the offsetWithinParent
     * @param size               the size
     */
    public SmallStringField(int offsetWithinParent, int size) {
      super("F str8", offsetWithinParent, size, 1);
      if (size > Byte.MAX_VALUE) {
        throw new IllegalArgumentException();
      }
      this.len = new Int8Field(getOffsetWithinStruct(), 1);
    }

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
   * The type Medium string field. short-based length.
   *
   * @author cschanck
   */
  public static class MediumStringField extends AbstractSizedStringField {

    private final Int16Field len;

    /**
     * Instantiates a new Abstract struct field.
     *
     * @param offsetWithinParent the offsetWithinParent
     * @param size               the size
     */
    public MediumStringField(int offsetWithinParent, int size) {
      super("F str16", offsetWithinParent, size, 2);
      if (size > Short.MAX_VALUE) {
        throw new IllegalArgumentException();
      }
      this.len = new Int16Field(getOffsetWithinStruct(), 1);
    }

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
   * The type Large string field. int based length.
   *
   * @author cschanck
   */
  public static class LargeStringField extends AbstractSizedStringField {

    private final Int32Field len;

    /**
     * Instantiates a new Abstract struct field.
     *
     * @param offsetWithinParent the offsetWithinParent
     * @param size               the size
     */
    public LargeStringField(int offsetWithinParent, int size) {
      super("F str32", offsetWithinParent, size, 4);
      this.len = new Int32Field(getOffsetWithinStruct(), 1);
    }

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
