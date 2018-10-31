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
package com.terracottatech.sovereign.btrees.duplicate;

import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.StructBuilder;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int64Field;

/**
 * @author cschanck
 */
public class LongListProxy {

  private static final Int16Field SIZE_FIELD;
  private static final Int64Field ARRAY_FIELD;
  private static final Int64Field REV_FIELD;

  static {
    StructBuilder b = new StructBuilder();
    SIZE_FIELD = b.int16();
    REV_FIELD = b.int64();
    ARRAY_FIELD = b.int64();
  }

  public static final LongListProxy PROXY = new LongListProxy();

  public static int listSizeForPageSize(int pgSize) {
    return (pgSize - SIZE_FIELD.getAllocatedSize() - REV_FIELD.getAllocatedSize()) / ARRAY_FIELD.getAllocatedSize();
  }

  public static int byteSizeForListCount(int cnt) {
    return SIZE_FIELD.getAllocatedSize() + REV_FIELD.getAllocatedSize() + (cnt * ARRAY_FIELD.getAllocatedSize());
  }

  public int size(Accessor a) {
    return SIZE_FIELD.get(a);
  }

  public long get(Accessor a, int index) {
    return ARRAY_FIELD.get(a, index);
  }

  public long getRevision(Accessor a) {
    return REV_FIELD.get(a);
  }

  public void insert(int sz, Accessor a, int index, long val) {
    ARRAY_FIELD.move(a, index, index + 1, sz - index);
    ARRAY_FIELD.put(a, index, val);
  }

  public void delete(int sz, Accessor a, int index) {
    ARRAY_FIELD.move(a, index + 1, index, sz - index - 1);
  }

  public void set(Accessor a, int index, long val) {
    ARRAY_FIELD.put(a, index, val);
  }

  public void setRevision(Accessor a, long rev) {
    REV_FIELD.put(a, rev);
  }

  public void setSize(Accessor a, short i) {
    SIZE_FIELD.put(a, i);
  }
}
