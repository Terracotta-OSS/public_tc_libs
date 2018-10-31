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

import com.terracottatech.sovereign.spi.SpaceRuntime;

import java.nio.ByteBuffer;

/**
 * @author mscott
 */
public interface DataContainer<C extends Context, L extends Locator, W> extends ValueGenerator<L> {

  /**
   * Runtime for this container.
   *
   * @return
   */
  SpaceRuntime runtime();

  /**
   * @param data to add to a container
   * @return pointer to data location
   */
  L add(W data);

  /**
   * Reinstall the data, as if on restart. What is passed to the tap for persistence
   * will return as data.
   *
   * @param lsn
   * @param data
   */
  L reinstall(long lsn, long persistentKey, ByteBuffer data);

  /**
   * remove data
   *
   * @param key pointer to data returned at data add
   * @return true if data is deleted
   */
  boolean delete(L key);

  /**
   * replace data atomically
   *
   * @param key pointer to data returned at data add
   * @return true if data is replaced
   */
  L replace(L key, W data);

  /**
   * @param key pointer returned when the data is added
   * @return data from the container
   */
  W get(L key);

  /**
   * @return the pointer to the first location of valid data
   */
  L first(C context);

  /**
   * @return last location of valid data
   */
  L last();

  /**
   * drop this container.  Does not remove secondary structures.
   */
  void drop();

  /**
   * Marks this container as disposed making this container unusable.  Following
   * a call to this method, calls to other methods <i>may</i> throw an
   * {@link java.lang.IllegalStateException}.
   */
  void dispose();

  /**
   * Indicates if this {@code DataContainer} is undergoing disposal or is presently disposed and no longer usable.
   *
   * @return {@code true} if the {@link #dispose} method has been called whether or not the disposal
   * operation is complete; {@code false} otherwise
   */
  boolean isDisposed();

  /**
   * Retrive a context to use for an operation
   * @param guard if true, impl must guard against this context not being closed
   * before object gc.
   * @return
   */
  C start(boolean guard);

  void end(C loc);

  long count();

  long getUsed();

  long getReserved();
}
