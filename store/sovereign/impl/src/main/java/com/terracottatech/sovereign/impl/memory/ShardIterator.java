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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implemented specifically only for iterating over a shard for passive sync.
 * This class should not be used as a standard iterator.
 */
public class ShardIterator implements Iterator<BufferDataTuple>, AutoCloseable {

  private final AbstractRecordContainer<?> recordContainer;
  private final ContextImpl context;
  private PersistentMemoryLocator nextloc;

  public ShardIterator(AbstractRecordContainer<?> recordContainer) {
    this.recordContainer = recordContainer;
    context = recordContainer.start(false);
    nextloc = recordContainer.first(context);
  }

  @Override
  public boolean hasNext() {
    return nextloc.index() >= 0;
  }

  @Override
  public BufferDataTuple next() {
    final long index = nextloc.index();
    final ByteBuffer byteBuffer = recordContainer.get(index);
    if (index != -1) {
      BufferDataTuple ret = new BufferDataTuple() {
        @Override
        public long index() {
          return index;
        }

        @Override
        public ByteBuffer getData() {
          return byteBuffer;
        }
      };
      nextloc = nextloc.next();
      return ret;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void close() {
    context.close();
  }
}
