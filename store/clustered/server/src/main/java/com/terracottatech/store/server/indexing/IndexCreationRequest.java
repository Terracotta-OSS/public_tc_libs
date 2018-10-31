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
package com.terracottatech.store.server.indexing;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ExplicitRetirementHandle;
import org.terracotta.entity.MessageCodecException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a pending index creation request.  If multiple clients request creation of the same
 * index (before index creation is completed), the requests are combined.
 * @param <T> the index key type
 */
@CommonComponent
public class IndexCreationRequest<T extends Comparable<T>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexCreationRequest.class);

  private final Map.Entry<CellDefinition<T>, IndexSettings> requestKey;
  private final Map<UUID, ExplicitRetirementHandle<?>> retirementHandles = new LinkedHashMap<>();
  private final String alias;

  private volatile SovereignIndex<T> sovereignIndex;
  private volatile Throwable fault;
  private volatile boolean retry = false;

  public IndexCreationRequest(Map.Entry<CellDefinition<T>, IndexSettings> requestKey, String alias) {
    this.requestKey = requestKey;
    this.alias = alias;
  }

  public synchronized void reference(UUID requesterId, ExplicitRetirementHandle<?> handle) {
    retirementHandles.put(requesterId, handle);
  }

  public synchronized ExplicitRetirementHandle<?> dereference(UUID requesterId) {
    return retirementHandles.remove(requesterId);
  }

  public void setRetry() {
    this.retry = true;
  }

  public boolean isRetry() {
    return retry;
  }

  public synchronized boolean isPendingFor(UUID requesterId) {
    return retirementHandles.containsKey(requesterId);
  }

  public void complete(SovereignIndex<T> index) {
    this.retry = false;
    this.sovereignIndex = index;
  }

  public Map.Entry<CellDefinition<T>, IndexSettings> getRequestKey() {
    return requestKey;
  }

  public Map<UUID, ExplicitRetirementHandle<?>> getRetirementHandles() {
    return retirementHandles;
  }

  public SovereignIndex<T> getSovereignIndex() {
    return sovereignIndex;
  }

  public void complete(Throwable fault) {
    this.retry = false;
    this.fault = fault;
  }

  public Throwable getFault() {
    return fault;
  }

  public synchronized void release() {
    retirementHandles.forEach((key, value) -> {
      try {
        value.release();
      } catch (MessageCodecException e) {
        LOGGER.error("Unable to release response to '{}[{}:{}]' index creation by {}",
                alias, requestKey.getValue(), requestKey.getKey(), key, e);
          /*
           * TODO: Determine proper response
           * This exception should **NOT** happen but the client is **hung** awaiting response ...
           */
      }
    });
  }

  @Override
  public String toString() {
    return "IndexCreationRequest{" +
            "requestKey=" + requestKey +
            ", sovereignIndex=" + sovereignIndex +
            ", fault=" + fault +
            ", retirementHandles=" + retirementHandles +
            '}';
  }

}
