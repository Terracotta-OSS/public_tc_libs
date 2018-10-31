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
package com.terracottatech.store.server;

import org.terracotta.client.message.tracker.OOOMessageHandler;
import org.terracotta.entity.ClientSourceId;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.StateDumpCollector;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class RecordingMessageHandler implements OOOMessageHandler<DatasetEntityMessage, DatasetEntityResponse> {

  private final Predicate<DatasetEntityMessage> trackerPolicy = new DatasetTrackerPolicy();
  Map<Long, DatasetEntityResponse> trackedResponses = new HashMap<>();

  @Override
  public DatasetEntityResponse invoke(InvokeContext invokeContext, DatasetEntityMessage datasetEntityMessage,
                                      BiFunction<InvokeContext, DatasetEntityMessage, DatasetEntityResponse> biFunction) throws EntityUserException {
    if (trackerPolicy.test(datasetEntityMessage)) {
      return trackedResponses.computeIfAbsent(invokeContext.getCurrentTransactionId(), key -> biFunction.apply(invokeContext, datasetEntityMessage));
    } else {
      return biFunction.apply(invokeContext, datasetEntityMessage);
    }
  }

  @Override
  public void untrackClient(ClientSourceId clientSourceId) {

  }

  @Override
  public Stream<ClientSourceId> getTrackedClients() {
    throw new UnsupportedOperationException("Do not use this");
  }

  @Override
  public Map<Long, DatasetEntityResponse> getTrackedResponsesForSegment(int index, ClientSourceId clientSourceId) {
    return trackedResponses;
  }

  @Override
  public void loadTrackedResponsesForSegment(int index, ClientSourceId clientSourceId, Map<Long, DatasetEntityResponse> trackedResponses) {
    this.trackedResponses.putAll(trackedResponses);
  }

  @Deprecated
  @Override
  public void loadOnSync(ClientSourceId clientSourceId, Map<Long, DatasetEntityResponse> trackedResponses) {
    throw new UnsupportedOperationException("Do not use this");
  }

  @Override
  public void addStateTo(StateDumpCollector stateDumpCollector) {

  }
}
