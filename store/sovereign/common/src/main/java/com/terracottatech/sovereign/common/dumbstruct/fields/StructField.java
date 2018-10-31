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
public interface StructField {

  /**
   * Gets offset within struct.
   *
   * @return the offset within struct
   */
  int getOffsetWithinStruct();

  /**
   * Gets allocated size.
   *
   * @return the allocated size
   */
  int getAllocatedSize();

  /**
   * Gets number of fields.
   *
   * @return the number of fields
   */
  int getAllocationCount();

  /**
   * Gets single field size.
   *
   * @return the single field size
   */
  int getSingleFieldSize();

  /**
   * Move within the field array
   *
   * @param a
   * @param fromIndex
   * @param toIndex
   * @param many
   */
  void move(Accessor a, int fromIndex, int toIndex, int many);

  /**
   * Calculate byte offset for a specific index in an array.
   *
   * @param index 0-based index
   * @return byte offset.
   */
  int deltaFor(int index);

}
