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
package com.terracottatech.sovereign.spi.store;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implementations of this interface are intended to be immutable pointers to
 * data in the container.  Ideally there is enough information in the pointer
 * to ensure uniqueness of data.  It would not be a good idea to have stale pointers
 * returning information.
 * <p>
 * Implementations of this interface are intended to use {@link com.terracottatech.sovereign.spi.store
 * .LocatorFactory} instances to manage global shared resources, and provide unidirectional next() support.
 *
 * @author mscott
 */
public interface Locator extends Iterable<Locator> {

  enum TraversalDirection {
    NONE(false, false),
    FORWARD(true, false),
    REVERSE(false, true);

    private final boolean reverse;
    private final boolean forward;

    TraversalDirection(boolean forward, boolean reverse) {
      this.forward = forward;
      this.reverse = reverse;
    }

    public boolean isReverse() {
      return reverse;
    }

    public boolean isForward() {
      return forward;
    }

  }

  /**
   * @return next valid pointer to data
   */
  Locator next();

  /**
   * @return endpoint of data.  either the front or the back.  not a valid pointer
   */
  boolean isEndpoint();

  /**
   * Is it valid.
   *
   * @return
   */
  default boolean isValid() {
    return !isEndpoint();
  }

  /**
   * Direction this Locator can traverse
   *
   * @return
   */
  TraversalDirection direction();

  @Override
  public default Iterator<Locator> iterator() {
    return new Iterator<Locator>() {
      Locator position = Locator.this;

      @Override
      public boolean hasNext() {
        return position.isValid();
      }

      @Override
      public Locator next() {
        if (position.isValid()) {
          try {
            return position;
          } finally {
            position = position.next();
          }
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

}
