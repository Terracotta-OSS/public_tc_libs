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
package com.terracottatech.sovereign.impl.memory;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

//Intended to be always used under shard's lock
public class RecordContainerChangeListener {

  private final Set<ChangeListener> changeListeners = new HashSet<>();

  public void listen(PersistentMemoryLocator locator, ByteBuffer data) {
    this.changeListeners.forEach(changeListener -> changeListener.listen(locator, data));
  }

  void addChangeListener(ChangeListener listener) {
    changeListeners.add(listener);
  }

  void removeChangeListener(ChangeListener listener) {
    listener.getChanges().clear();
    if(!changeListeners.remove(listener)) {
      throw new IllegalStateException("No Mutations Recorded");
    }
  }

  public static class ChangeListener {

    private final List<BufferDataTuple> changes = new LinkedList<>();

    List<BufferDataTuple> getChanges() {
      return changes;
    }

    void listen(PersistentMemoryLocator locator, ByteBuffer data) {
      this.changes.add(new BufferDataTuple() {
        @Override
        public long index() {
          return locator.index();
        }

        @Override
        public ByteBuffer getData() {
          if (data != null) {
            return data.duplicate();
          } else {
            return null;
          }
        }
      });
    }
  }

}
