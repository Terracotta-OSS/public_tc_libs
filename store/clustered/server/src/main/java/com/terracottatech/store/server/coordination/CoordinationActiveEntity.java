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
package com.terracottatech.store.server.coordination;

import com.terracottatech.store.Tuple;
import com.terracottatech.store.common.coordination.CoordinationMessage;
import com.terracottatech.store.common.coordination.CoordinatorMessaging;
import com.terracottatech.store.common.coordination.JoinRequest;
import com.terracottatech.store.common.coordination.LeadershipRelease;
import com.terracottatech.store.common.coordination.LeadershipRequest;
import com.terracottatech.store.common.coordination.StateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityUserException;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ReconnectRejectedException;
import org.terracotta.entity.StateDumpCollector;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.time.Instant.now;
import static java.util.stream.Collectors.toList;

public class CoordinationActiveEntity implements ActiveServerEntity<CoordinationMessage, StateResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationActiveEntity.class);

  private final ConcurrentMap<String, Tuple<UUID, ClientDescriptor>> members = new ConcurrentHashMap<>();
  private final ClientCommunicator communicator;

  private volatile StateResponse.LeadershipState leader;

  public CoordinationActiveEntity(ClientCommunicator communicator) {
    this.communicator = communicator;
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    //nothing to do
  }

  @Override
  public void disconnected(ClientDescriptor client) {
    try {
      // Disconnection does not automatically relinquish leadership
      this.members.entrySet().stream().filter(e -> client.equals(e.getValue().getSecond())).collect(toList())
          .forEach(entry -> members.remove(entry.getKey(), entry.getValue()));
      broadcastState(new StateResponse(this.members.keySet(), leader));
      LOGGER.debug("Disconnected: {}", client);
      LOGGER.trace("Post disconnected state {}, leader {}", members, leader);
    } catch (AssertionError e) {
      LOGGER.warn("Assertion!!!", e);
    }
  }

  @Override
  public StateResponse invokeActive(ActiveInvokeContext<StateResponse> context, CoordinationMessage message) throws EntityUserException {
    try {
      ClientDescriptor client = context.getClientDescriptor();
      if (message instanceof JoinRequest) {
        UUID identity = ((JoinRequest) message).getIdentity();
        String name = ((JoinRequest) message).getName();
        join(client, identity, name);
        LOGGER.debug("Joined: {}:{} as {}", client, identity, name);
        LOGGER.trace("Post joined state {}, leader {}", members, leader);
      } else if (message instanceof LeadershipRequest) {
        Duration leaseLength = ((LeadershipRequest) message).getLeaseLength();
        requestLeadership(client, leaseLength);
        LOGGER.debug("Leadership Request: {} for {}", client, leaseLength);
        LOGGER.trace("Post leadership request state {}, leader {}", members, leader);
      } else if (message instanceof LeadershipRelease) {
        if (leader != null && leader.getName().equals(nameOf(client))) {
          leader = null;
        }
        LOGGER.debug("Leadership Release: {}", client);
        LOGGER.trace("Post leadership release state {}, leader {}", members, leader);
      }
      return broadcastState(new StateResponse(members.keySet(), leader));
    } catch (AssertionError e) {
      throw new EntityUserException("Assertion!!!", e);
    }
  }

  private void join(ClientDescriptor client, UUID identity, String name) throws EntityUserException {
    Tuple<UUID, ClientDescriptor> membership = Tuple.of(identity, client);

    Tuple<UUID, ClientDescriptor> existingHolder = members.putIfAbsent(name, membership);
    if (existingHolder != null) {
      if (existingHolder.getFirst().equals(identity)) {
        if (!members.replace(name, existingHolder, membership)) {
          throw new AssertionError("Impossible membership race condition?");
        }
      } else {
        throw new EntityUserException("Membership already owned by " + existingHolder + " cannot be claimed by " + client);
      }
    }
  }

  private void requestLeadership(ClientDescriptor client, Duration leaseLength) throws EntityUserException {
    if (leader == null || members.get(leader.getName()).getSecond().equals(client) ||
        (!members.containsKey(leader.getName()) && now().isAfter(leader.getExpiry()))) {
      leader = new StateResponse.LeadershipState(nameOf(client), now().plus(leaseLength));
    }
  }

  private StateResponse broadcastState(StateResponse state) {
    members.values().forEach(member -> {
      try {
        communicator.sendNoResponse(member.getSecond(), state);
      } catch (MessageCodecException e) {
        throw new AssertionError(e);
      }
    });
    return state;
  }

  private String nameOf(ClientDescriptor client) throws EntityUserException {
    return members.entrySet().stream().filter(e -> client.equals(e.getValue().getSecond())).map(Map.Entry::getKey).reduce((a, b) -> {
      throw new AssertionError("Client has multiple identities: " + client + " [" + a + " & " + b + "]");
    }).orElseThrow(() -> new EntityUserException("A client has no name: " + client));
  }

  @Override
  public void loadExisting() {
    //no state to load
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<CoordinationMessage> syncChannel, int concurrencyKey) {
    //no state to sync
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (client, data) -> {
      try {
        Tuple<JoinRequest, LeadershipRequest> reconnect = CoordinatorMessaging.decodeReconnect(data);
        UUID identity = reconnect.getFirst().getIdentity();
        String name = reconnect.getFirst().getName();
        join(client, identity, name);

        LeadershipRequest leadershipRequest = reconnect.getSecond();
        if (leadershipRequest == null) {
          LOGGER.debug("Client reconnect: {}:{} as {}", client, identity, name);
        } else {
          Duration leaseLength = leadershipRequest.getLeaseLength();
          requestLeadership(client, leaseLength);
          LOGGER.debug("Client reconnect: {}:{} as {} [leader for {}]", client, identity, name, leaseLength);
        }
        LOGGER.trace("Post client reconnect state {}, leader {}", members, leader);
      } catch (AssertionError e) {
        throw new ReconnectRejectedException("Assertion!!!", e);
      } catch (EntityUserException e) {
        throw new ReconnectRejectedException("Client has identity issues", e);
      } catch (MessageCodecException | IOException e) {
        throw new ReconnectRejectedException("Codec problem", e);
      }
    };
  }

  @Override
  public void createNew() throws ConfigurationException {

  }

  @Override
  public void destroy() {

  }

  @Override
  public void addStateTo(StateDumpCollector stateDumpCollector) {
    StateDumpCollector membersDump = stateDumpCollector.subStateDumpCollector("members");
    for (Map.Entry<String, Tuple<UUID, ClientDescriptor>> m : members.entrySet()) {
      membersDump.addState(m.getKey(), m.getValue().getSecond() + " as " + m.getValue().getFirst());
    }
    if (leader == null) {
      stateDumpCollector.addState("leader", "<none>");
    } else {
      stateDumpCollector.addState("leader", leader.getName());
      stateDumpCollector.addState("leader-expires-at", leader.getExpiry());
    }
  }
}
