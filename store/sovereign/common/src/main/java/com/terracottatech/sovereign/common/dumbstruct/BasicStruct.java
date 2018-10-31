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

import com.terracottatech.sovereign.common.dumbstruct.fields.AbstractStructField;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Basic implementation of a structure.
 *
 * @author cschanck
 */
public class BasicStruct extends AbstractStructField implements Struct {
  private final List<StructField> fields;

  /**
   * Instantiates a new Basic struct.
   *
   * @param offset the offset
   * @param many the many
   * @param fields the fields
   */
  public BasicStruct(int offset, int many, List<StructField> fields) {
    super("Struct ", offset, many, getTotalSize(fields));
    if (new HashSet<>(fields).size() != fields.size()) {
      throw new IllegalStateException();
    }
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  private static int getTotalSize(Collection<StructField> fields) {
    int size = 0;
    for (StructField p : fields) {
      size = size + p.getAllocatedSize();
    }
    return size;
  }

  /**
   * Gets fields.
   *
   * @return the fields
   */
  @Override
  public List<StructField> getFields() {
    return fields;
  }
}
