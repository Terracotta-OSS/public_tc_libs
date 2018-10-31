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
package com.terracottatech.ehcache.clustered.client.internal;

import com.terracottatech.ehcache.clustered.client.internal.config.ConfigurationSplitter;
import com.terracottatech.entity.AggregateEndpoint;

import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.internal.service.ClusterTierException;
import org.ehcache.clustered.client.internal.store.InternalClusterTierClientEntity;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheOperationMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;

import java.util.HashSet;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.abs;

/**
 * AggregatingClusterTierClientEntity
 */
public class AggregatingClusterTierClientEntity implements InternalClusterTierClientEntity {

  private final ConfigurationSplitter configSplitter = new ConfigurationSplitter();
  private final AggregateEndpoint<InternalClusterTierClientEntity> endpoint;
  private final int stripeCount;

  public AggregatingClusterTierClientEntity(AggregateEndpoint<InternalClusterTierClientEntity> endpoint) {
    this.endpoint = endpoint;
    stripeCount = endpoint.getEntities().size();
  }

  @Override
  public boolean isConnected() {
    for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
      if (!singleEntity.isConnected()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void validate(ServerStoreConfiguration clientStoreConfiguration) throws ClusterTierException, TimeoutException {
    ServerStoreConfiguration splitConfiguration = configSplitter.splitServerStoreConfiguration(clientStoreConfiguration, stripeCount);
    for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
      singleEntity.validate(splitConfiguration);
    }
  }

  @Override
  public Timeouts getTimeouts() {
    return endpoint.getEntities().get(0).getTimeouts();
  }

  @Override
  public void invokeAndWaitForSend(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    if (message instanceof ServerStoreOpMessage.KeyBasedServerStoreOpMessage) {
      ServerStoreOpMessage.KeyBasedServerStoreOpMessage keyBasedMessage = (ServerStoreOpMessage.KeyBasedServerStoreOpMessage) message;
      getEntityAtIndex(getStripeIndexFor(keyBasedMessage.getKey())).invokeAndWaitForSend(message, track);
    } else {
      for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
        singleEntity.invokeAndWaitForSend(message, track);
      }
    }
  }

  @Override
  public void invokeAndWaitForReceive(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    if (message instanceof ServerStoreOpMessage.KeyBasedServerStoreOpMessage) {
      ServerStoreOpMessage.KeyBasedServerStoreOpMessage keyBasedMessage = (ServerStoreOpMessage.KeyBasedServerStoreOpMessage) message;
      getEntityAtIndex(getStripeIndexFor(keyBasedMessage.getKey())).invokeAndWaitForReceive(message, track);
    } else {
      for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
        singleEntity.invokeAndWaitForReceive(message, track);
      }
    }
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForComplete(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    EhcacheEntityResponse response = null;
    if (message instanceof ServerStoreOpMessage.KeyBasedServerStoreOpMessage) {
      ServerStoreOpMessage.KeyBasedServerStoreOpMessage keyBasedMessage = (ServerStoreOpMessage.KeyBasedServerStoreOpMessage) message;
      response = getEntityAtIndex(getStripeIndexFor(keyBasedMessage.getKey())).invokeAndWaitForComplete(message, track);
    } else {
      for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
        response = singleEntity.invokeAndWaitForComplete(message, track);
      }
    }
    return response;
  }

  @Override
  public EhcacheEntityResponse invokeAndWaitForRetired(EhcacheOperationMessage message, boolean track) throws ClusterException, TimeoutException {
    EhcacheEntityResponse response = null;
    if (message instanceof ServerStoreOpMessage.KeyBasedServerStoreOpMessage) {
      ServerStoreOpMessage.KeyBasedServerStoreOpMessage keyBasedMessage = (ServerStoreOpMessage.KeyBasedServerStoreOpMessage) message;
      response = getEntityAtIndex(getStripeIndexFor(keyBasedMessage.getKey())).invokeAndWaitForRetired(message, track);
    } else {
      for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
        response = singleEntity.invokeAndWaitForRetired(message, track);
      }
    }
    return response;
  }

  @Override
  public EhcacheEntityResponse invokeStateRepositoryOperation(StateRepositoryOpMessage message, boolean track) throws ClusterException, TimeoutException {
    return getFirstEntity().invokeStateRepositoryOperation(message, track);
  }

  @Override
  public <T extends EhcacheEntityResponse> void addResponseListener(Class<T> responseType, ResponseListener<T> responseListener) {
    for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
      singleEntity.addResponseListener(responseType, responseListener);
    }
  }

  @Override
  public void addDisconnectionListener(final DisconnectionListener disconnectionListener) {
    DisconnectionListener wrappingListener = () -> {
      close();
      disconnectionListener.onDisconnection();
    };
    for (InternalClusterTierClientEntity singleEntity : endpoint.getEntities()) {
      singleEntity.addDisconnectionListener(wrappingListener);
    }
  }

  @Override
  public void setReconnectListener(final ReconnectListener reconnectListener) {
    for (int i = 0; i < stripeCount; i++) {
      endpoint.getEntities().get(i).setReconnectListener(new FilteringReconnectListener(reconnectListener, i));
    }
  }

  @Override
  public void close() {
    endpoint.close();
  }

  private InternalClusterTierClientEntity getFirstEntity() {
    return endpoint.getEntities().get(0);
  }

  private InternalClusterTierClientEntity getEntityAtIndex(int index) {
    if (index < 0 || index > stripeCount) {
      throw new AssertionError("Index must be between 0 and " + stripeCount + " - received " + index);
    }
    return endpoint.getEntities().get(index);
  }

  private int getStripeIndexFor(long key) {
      key = (~key) + (key << 18);
      key = key ^ (key >>> 31);
      key = key * 21;
      key = key ^ (key >>> 11);
      key = key + (key << 6);
      key = key ^ (key >>> 22);
      return abs((int) key) % stripeCount;
  }

  private class FilteringReconnectListener implements ReconnectListener {

    private final ReconnectListener delegate;
    private final int index;

    private FilteringReconnectListener(ReconnectListener delegate, int index) {
      this.delegate = delegate;
      this.index = index;
    }

    @Override
    public void onHandleReconnect(ClusterTierReconnectMessage reconnectMessage) {
      ClusterTierReconnectMessage message = new ClusterTierReconnectMessage(reconnectMessage.getInvalidationsInProgress());
      delegate.onHandleReconnect(message);
      HashSet<Long> hashes = new HashSet<>();
      for (Long hash : message.getInvalidationsInProgress()) {
        if (getStripeIndexFor(hash) == index) {
          hashes.add(hash);
        }
      }
      reconnectMessage.addInvalidationsInProgress(hashes);
    }
  }
}
