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

import com.terracottatech.sovereign.SovereignDataset;

import java.util.concurrent.Callable;

/**
 * A {@link TimeReferenceGenerator} that participates in the dataset metadata persistence mechanism
 * provided by {@link SovereignDataset}.
 * <p>
 * Stateful {@code TimeReferenceGenerator} instances must <b>not</b> be shared among multiple
 * {@code SovereignDataset} instances.
 *
 * @author Clifford W. Johnson
 */
public interface PersistableTimeReferenceGenerator<Z extends TimeReference<Z>> extends TimeReferenceGenerator<Z> {

  /**
   * Sets the callback through which this {@code PersistableTimeReferenceGenerator} may signal that its
   * state should be saved.  Since a {@code PersistableTimeReferenceGenerator} instance should be used by
   * no more than one dataset, implementations of this method should throw an {@code IllegalStateException}
   * if more than one call to this method is made.
   * <p>
   * This method is only called during dataset setup/configuration.  The {@code Callback} provided should
   * be called by this {@code PersistableTimeReferenceGenerator} when its internal state must be saved.
   * When {@code persistenceCallback} is invoked, this {@code PersistableTimeReferenceGenerator}
   * instance is serialized along with the other dataset metadata and recorded in the persistence
   * medium.  This operation is expensive; it is recommended that the {@code persistenceCallback}
   * <b>not</b> be invoked for every {@code TimeReference} instance generated.  If the
   * {@code SovereignDataset} is not persisted, the {@code persistenceCallback} provided will be
   * an effective NO-OP.
   *
   * @param persistenceCallback the callback to be used by this {@code TimeReferenceGenerator}
   *                            to signal its state should be saved
   *
   * @throws NullPointerException if {@code persistenceCallback} is {@code null}
   * @throws IllegalStateException if this {@code TimeReferenceGenerator} already has a persistence callback
   */
  void setPersistenceCallback(Callable<Void> persistenceCallback);
}
