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
package com.terracottatech.store.client.stream.batched;

import com.terracottatech.store.Record;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.client.RecordImpl;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.stream.RootStreamDescriptor;
import com.terracottatech.store.common.messages.stream.Element;

import java.util.Spliterator;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Defines a {@link Spliterator} returning {@link Record} instances sourced from a remote
 * {@link com.terracottatech.store.Dataset Dataset} accessed via a {@link DatasetEntity}.
 *
 * @param <K> the key type of the records
 */
public class BatchedRemoteRecordSpliterator<K extends Comparable<K>>
    extends AbstractBatchedRemoteSpliterator<Record<K>> {

  /**
   * Creates a new {@code BatchedRemoteRecordSpliterator} against the remote {@link com.terracottatech.store.Dataset Dataset}.
   *
   * @param messageSender the {@code MessageSender} associated with the target dataset
   * @param streamId the identifier of the opened stream
   * @param descriptor a descriptor of the root {@link Stream} for which the {@code Spliterator} was opened
   * @param closeUsingServer a {@code BooleanSupplier} indicating whether or not the {@link #close()} method
   *                         should use server operations or not; if the value from
   *                         {@link BooleanSupplier#getAsBoolean() closeUsingServer.getAsBoolean} is {@code true},
   *                         server operations are used, otherwise, the server is not contacted
   */
  public BatchedRemoteRecordSpliterator(MessageSender messageSender, UUID streamId, RootStreamDescriptor descriptor,
                                         BooleanSupplier closeUsingServer) {
    super(messageSender, streamId, descriptor, closeUsingServer);
  }

  @Override
  public int characteristics() {
    return super.characteristics() | Spliterator.DISTINCT;
  }

  @Override
  protected Record<K> decodeElement(Element element) {
    return new RecordImpl<K>(element.getRecordData());
  }
}
