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

import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.spi.store.LocatorFactory;

/**
 * As to layout. Negative slot is invalid.
 * Bit 62 is reserved to mark hybrid storage.
 * Bits 61-N are reserved for shard index.
 *
 * @author mscott
 */
public class PersistentMemoryLocator implements Locator {

  private final long index;
  private final LocatorFactory factory;

  public PersistentMemoryLocator(long slot, LocatorFactory factory) {
    this.factory = factory;
    this.index = slot;
  }

  @Override
  public String toString() {
    return "PersistentMemoryLocator{" + +index + '}';
  }

  public long index() {
    return index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PersistentMemoryLocator locators = (PersistentMemoryLocator) o;

    return index == locators.index;

  }

  @Override
  public int hashCode() {
    return (int) (index ^ (index >>> 32));
  }

  @Override
  public synchronized PersistentMemoryLocator next() {
    // TODO analyze whether this needs to be synchronized.
    switch (direction()) {
      case FORWARD:
        return (PersistentMemoryLocator) factory.createNext();
      case REVERSE:
        return (PersistentMemoryLocator) factory.createPrevious();
      default:
        return PersistentMemoryLocator.INVALID;
    }
  }

  @Override
  public boolean isEndpoint() {
    return false;
  }

  @Override
  public TraversalDirection direction() {
    if (factory == null) {
      return TraversalDirection.NONE;
    }
    return factory.direction();
  }

  public LocatorFactory factory() {
    return factory;
  }

  public static final PersistentMemoryLocator INVALID = new PersistentMemoryLocator(-1, null) {
    @Override
    public boolean isEndpoint() {
      return true;
    }

    @Override
    public TraversalDirection direction() {
      return TraversalDirection.NONE;
    }

    @Override
    public PersistentMemoryLocator next() {
      return this;
    }

  };
}
