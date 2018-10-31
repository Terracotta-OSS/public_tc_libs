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
package com.terracottatech.store.common.coordination;

import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.store.Tuple;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.terracottatech.store.common.coordination.CoordinatorMessaging.decodeReconnect;
import static com.terracottatech.store.common.coordination.CoordinatorMessaging.encodeReconnect;
import static java.lang.Character.toChars;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Basic tests for {@link CoordinatorMessaging}.
 */
public class CoordinatorMessagingTest {

  @Test
  public void testReconnectDataAsMember() throws Exception {

    String name = "fooMember";
    UUID identity = UUID.randomUUID();

    Tuple<JoinRequest, LeadershipRequest> tuple = decodeReconnect(
        encodeReconnect(name, identity, null));

    assertThat(tuple.getFirst().getName(), is(name));
    assertThat(tuple.getFirst().getIdentity(), is(identity));
    assertThat(tuple.getSecond(), is(nullValue()));
  }

  @Test
  public void testReconnectDataAsLeader() throws Exception {

    String name = "fooMember";
    UUID identity = UUID.randomUUID();
    Duration originalLeaseLength = Duration.ofMinutes(2L);
    Instant expiry = Instant.now().plus(originalLeaseLength);
    StateResponse.LeadershipState leadershipState = new StateResponse.LeadershipState(name, expiry);

    Tuple<JoinRequest, LeadershipRequest> tuple = decodeReconnect(
        encodeReconnect(name, identity, leadershipState));

    assertThat(tuple.getFirst().getName(), is(name));
    assertThat(tuple.getFirst().getIdentity(), is(identity));
    assertThat(tuple.getSecond().getLeaseLength(), is(allOf(greaterThan(Duration.ZERO), lessThanOrEqualTo(originalLeaseLength))));
  }

  @Test
  public void testJoinRequest() {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    long seed = System.nanoTime();
    Random rndm = new Random(seed);
    try {
      String name = rndm.ints().filter(Character::isValidCodePoint).limit(rndm.nextInt(100)).mapToObj(codepoint -> new String(toChars(codepoint))).collect(joining());
      UUID identity = UUID.randomUUID();

      JoinRequest message = (JoinRequest) codec.decodeMessage(codec.encodeMessage(new JoinRequest(name, identity)));

      assertThat(message.getName(), is(name));
      assertThat(message.getIdentity(), is(identity));
    } catch (Throwable t) {
      throw new AssertionError("Failed on seed " + seed, t);
    }
  }

  @Test
  public void testLeadershipRequest() {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    long seed = System.nanoTime();
    Random rndm = new Random(seed);
    try {
      Duration lease = Duration.ofMillis(Long.MAX_VALUE & rndm.nextLong());

      LeadershipRequest message = (LeadershipRequest) codec.decodeMessage(codec.encodeMessage(new LeadershipRequest(lease)));

      assertThat(message.getLeaseLength(), is(lease));
    } catch (Throwable t) {
      throw new AssertionError("Failed on seed " + seed, t);
    }
  }

  @Test
  public void testLeadershipRelease() throws MessageCodecException {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    LeadershipRelease message = (LeadershipRelease) codec.decodeMessage(codec.encodeMessage(LeadershipRelease.INSTANCE));

    assertThat(message, is(LeadershipRelease.INSTANCE));
  }

  @Test
  public void testStateRequest() throws MessageCodecException {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    StateRequest message = (StateRequest) codec.decodeMessage(codec.encodeMessage(StateRequest.INSTANCE));

    assertThat(message, is(StateRequest.INSTANCE));
  }

  @Test
  public void testStateResponseWithoutLeader() {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    long seed = System.nanoTime();
    Random rndm = new Random(seed);
    try {
      Set<String> members = generate(
          () -> rndm.ints().filter(Character::isValidCodePoint).limit(rndm.nextInt(100))
              .mapToObj(codepoint -> new String(toChars(codepoint))).collect(joining()))
          .limit(rndm.nextInt(100)).distinct().collect(Collectors.toSet());

      StateResponse message = codec.decodeResponse(codec.encodeResponse(new StateResponse(members, null)));

      assertThat(message.getMembers(), is(members));
      assertThat(message.getLeader(), is(empty()));
    } catch (Throwable t) {
      throw new AssertionError("Failed on seed " + seed, t);
    }
  }

  @Test
  public void testStateResponseWithLeader() {
    MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

    long seed = System.nanoTime();
    Random rndm = new Random(seed);
    try {
      Set<String> members = generate(
          () -> rndm.ints().filter(Character::isValidCodePoint).limit(rndm.nextInt(100))
              .mapToObj(codepoint -> new String(toChars(codepoint))).collect(joining()))
          .limit(1 + rndm.nextInt(100)).distinct().collect(Collectors.toSet());
      String leader = new ArrayList<>(members).get(rndm.nextInt(members.size()));
      Instant expiry = Instant.now().plus(Duration.ofMillis(Long.MAX_VALUE & rndm.nextLong()));
      StateResponse.LeadershipState leadership = new StateResponse.LeadershipState(leader, expiry);

      StateResponse message = codec.decodeResponse(codec.encodeResponse(new StateResponse(members, leadership)));

      assertThat(message.getMembers(), is(members));
      assertThat(message.getLeader().get().getName(), is(leader));
      assertThat(message.getLeader().get().getExpiry(), is(expiry));
    } catch (Throwable t) {
      throw new AssertionError("Failed on seed " + seed, t);
    }
  }
}