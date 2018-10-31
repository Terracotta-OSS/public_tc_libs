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
package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;

/**
 * Used to return status from a delete operation.
 *
 * @author cschanck
 */
class DeleteReturn extends BtreeEntry {
  private final boolean nodeChanged;

  public DeleteReturn() {
    this(0L, 0L, false);
  }

  public DeleteReturn(Long deletedKey, Long deletedValue) {
    this(deletedKey, deletedValue, true);
  }

  public DeleteReturn(Long deletedKey, Long deletedValue, boolean nodeChanged) {
    super(deletedKey, deletedValue);
    this.nodeChanged = nodeChanged;
  }

  public boolean isNodeChanged() {
    return nodeChanged;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    DeleteReturn aReturn = (DeleteReturn) o;

    if (nodeChanged != aReturn.nodeChanged) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (nodeChanged ? 1 : 0);
    return result;
  }
}
