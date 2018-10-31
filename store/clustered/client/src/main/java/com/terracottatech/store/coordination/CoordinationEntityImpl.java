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
package com.terracottatech.store.coordination;

import com.tc.exception.VoltronEntityUserExceptionWrapper;
import com.terracottatech.store.common.coordination.CoordinationMessage;
import com.terracottatech.store.common.coordination.CoordinatorMessaging;
import com.terracottatech.store.common.coordination.JoinRequest;
import com.terracottatech.store.common.coordination.LeadershipRelease;
import com.terracottatech.store.common.coordination.LeadershipRequest;
import com.terracottatech.store.common.coordination.StateRequest;
import com.terracottatech.store.common.coordination.StateResponse;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class CoordinationEntityImpl implements CoordinationEntity, EndpointDelegate<StateResponse> {

  private final EntityClientEndpoint<CoordinationMessage, StateResponse> endpoint;
  private final String name;
  private final UUID identity;
  private final Consumer<StateResponse> stateSink;
  private final Runnable reconnectTrigger;
  private final Object notificationLock = new Object();

  private volatile StateResponse.LeadershipState leadership;
  @SuppressWarnings("unchecked")
  CoordinationEntityImpl(EntityClientEndpoint<CoordinationMessage, StateResponse> endpoint, Object[] config) {
    this.endpoint = endpoint;
    this.name = (String) config[0];
    this.identity = (UUID) config[1];
    this.stateSink = (Consumer<StateResponse>) config[2];
    this.reconnectTrigger = (Runnable) config[3];
    endpoint.setDelegate(this);
    getState(new JoinRequest(name, identity));
  }

  @Override
  public void close() {
    endpoint.close();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Set<String> getMembers() {
    return getState(StateRequest.INSTANCE).getMembers();
  }

  @Override
  public Optional<String> leader() {
    return getState(StateRequest.INSTANCE).getLeader().map(StateResponse.LeadershipState::getName);
  }

  private StateResponse getState(CoordinationMessage message) {
    try {
      return getUninterruptibly(endpoint.beginInvoke().message(message).invoke());
    } catch (MessageCodecException e) {
      throw new AssertionError(e);
    } catch (VoltronEntityUserExceptionWrapper e) {
      throw new IllegalStateException(e.getCause());
    } catch (EntityException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public synchronized boolean acquireLeadership() {
    leadership = getState(new LeadershipRequest(Duration.ZERO)).getLeader().filter(l -> name().equals(l.getName())).orElse(null);
    return leadership != null;
  }

  @Override
  public synchronized boolean leaseLeadership(Duration lease) {
    leadership = getState(new LeadershipRequest(lease)).getLeader().filter(l -> name().equals(l.getName())).orElse(null);
    return leadership != null;
  }

  @Override
  public synchronized void releaseLeadership() {
    leadership = getState(LeadershipRelease.INSTANCE).getLeader().filter(l -> name().equals(l.getName())).orElse(null);
  }

  private static <T extends EntityResponse> T getUninterruptibly(InvokeFuture<T> future) throws EntityException {
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void waitForChange(Duration waitPeriod) throws InterruptedException {
    synchronized (notificationLock) {
      notificationLock.wait(TimeUnit.SECONDS.toMillis(waitPeriod.getSeconds()), waitPeriod.getNano());
    }
  }

  @Override
  public void handleMessage(StateResponse state) {
    stateSink.accept(state);
    synchronized (notificationLock) {
      notificationLock.notifyAll();
    }
  }

  @Override
  public byte[] createExtendedReconnectData() {
    return CoordinatorMessaging.encodeReconnect(name, identity, leadership);
  }

  @Override
  public void didDisconnectUnexpectedly() {
    reconnectTrigger.run();
  }
}
