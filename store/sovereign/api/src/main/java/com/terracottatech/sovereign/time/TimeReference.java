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
package com.terracottatech.sovereign.time;

import com.terracottatech.store.Record;

import java.io.Externalizable;
import java.io.Serializable;

/**
 * Identifies the generic time reference object used in TC Store to associate a time value
 * with a {@link Record}.  Implementations are expected to be <i>immutable</i>.
 * <p>
 * A {@link TimeReferenceGenerator} implementation must be provided to create
 * {@code TimeReference} instances.  In addition, one of the following <b>must</b>
 * be done:
 * <ul>
 *   <li>the {@code TimeReference} implementation must implement either {@link Serializable} or
 * {@link Externalizable}</li>
 *   <li>the {@link TimeReferenceGenerator#put(ByteBuffer, TimeReference)} and
 * {@link TimeReferenceGenerator#get(ByteBuffer)} methods <b>must</b> overridden</li>
 * </ul>
 * <p>
 * A {@code TimeReference} instance may contain a node- or system-oriented component
 * indicating its origin.
 * <p>
 *
 * @param <Z> the type of the {@code TimeReference} implementation
 *
 * @author Clifford W. Johnson
 *
 * @see TimeReferenceGenerator
 */
public interface TimeReference<Z extends TimeReference<Z>> extends Comparable<TimeReference<?>> {

  /**
   * {@inheritDoc}
   * <p>
   * Implementations of this method <i>should</i> provide a total, chronological ordering over
   * {@code TimeReference} instances, consistent with the needs of the application, regardless
   * of the node or system of origin.  The implementation need <i>not</i> be consistent with
   * {@link Object#equals(Object) equals}.
   *
   * @param t the {@code TimeReference} with which {@code this} is compared
   *
   * @return a negative integer if {@code this} {@code TimeReference} comes <i>before</i> {@code t},
   *    a positive integer if {@code this} {@code TimeReference} comes <i>after</i> {@code t}, and
   *    zero otherwise
   *
   * @throws NullPointerException {@inheritDoc}
   * @throws ClassCastException {@inheritDoc}
   */
  @Override
  int compareTo(TimeReference<?> t);

}
