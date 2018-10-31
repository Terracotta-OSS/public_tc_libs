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

import com.terracottatech.store.Tuple;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.ArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.ArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;

public class CoordinatorMessaging implements MessageCodec<CoordinationMessage, StateResponse> {

  private static final Struct STATUS_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
      .string("leader", 10)
      .int64("expiry", 20)
      .strings("members", 30).build();

  private static final Struct COORDINATION_MESSAGE_STRUCT = StructBuilder.newStructBuilder()
      .string("name", 10)
      .string("identity", 20)
      .bool("leader", 30)
      .int64("lease", 40)
      .build();

  private static final MessageCodec<CoordinationMessage, StateResponse> INSTANCE = new CoordinatorMessaging();

  public static MessageCodec<CoordinationMessage, StateResponse> codec() {
    return INSTANCE;
  }

  private CoordinatorMessaging() {

  }

  @Override
  public byte[] encodeMessage(CoordinationMessage message) throws MessageCodecException {
    if (message instanceof JoinRequest) {
      return COORDINATION_MESSAGE_STRUCT.encoder()
          .string("name", ((JoinRequest) message).getName())
          .string("identity", ((JoinRequest) message).getIdentity().toString())
          .encode().array();
    } else if (message instanceof LeadershipRequest) {
      return COORDINATION_MESSAGE_STRUCT.encoder()
          .bool("leader", true)
          .int64("lease", ((LeadershipRequest) message).getLeaseLength().toMillis())
          .encode().array();
    } else if (message instanceof LeadershipRelease) {
      return COORDINATION_MESSAGE_STRUCT.encoder()
          .bool("leader", false)
          .encode().array();
    } else if (message instanceof StateRequest) {
      return COORDINATION_MESSAGE_STRUCT.encoder()
          .encode().array();
    } else {
      throw new IllegalArgumentException("Unrecognized message: " + message.getClass());
    }
  }

  @Override
  public CoordinationMessage decodeMessage(byte[] payload) throws MessageCodecException {
    StructDecoder<Void> decoder = COORDINATION_MESSAGE_STRUCT.decoder(ByteBuffer.wrap(payload));
    String name = decoder.string("name");
    if (name != null) {
      UUID identity = UUID.fromString(decoder.string("identity"));
      return new JoinRequest(name, identity);
    }
    Boolean leader = decoder.bool("leader");
    if (leader != null) {
      if (leader) {
        return new LeadershipRequest(Duration.ofMillis(decoder.int64("lease")));
      } else {
        return LeadershipRelease.INSTANCE;
      }
    }
    return StateRequest.INSTANCE;
  }

  @Override
  public byte[] encodeResponse(StateResponse response) throws MessageCodecException {
    Optional<StateResponse.LeadershipState> leader = response.getLeader();
    StructEncoder<Void> encoder = STATUS_RESPONSE_STRUCT.encoder();
    if (leader.isPresent()) {
      encoder = encoder.string("leader", leader.map(l -> l.getName()).orElse(null))
          .int64("expiry", leader.map(l -> l.getExpiry().toEpochMilli()).orElse(null));
    }
    ArrayEncoder<String, StructEncoder<Void>> memberEncoder = encoder.strings("members");
    response.getMembers().forEach(memberEncoder::value);

    return memberEncoder.end().encode().array();
  }

  @Override
  public StateResponse decodeResponse(byte[] payload) throws MessageCodecException {
    StructDecoder<Void> decoder = STATUS_RESPONSE_STRUCT.decoder(ByteBuffer.wrap(payload));
    String leader = decoder.string("leader");
    Instant expiry = ofNullable(decoder.int64("expiry")).map(Instant::ofEpochMilli).orElse(null);
    Set<String> members = new HashSet<>();
    ArrayDecoder<String, StructDecoder<Void>> memberDecoder = decoder.strings("members");
    int length = memberDecoder.length();
    for (int i = 0; i < length; i++) {
      members.add(memberDecoder.value());
    }
    if (leader == null) {
      return new StateResponse(members, null);
    } else {
      return new StateResponse(members, new StateResponse.LeadershipState(leader, expiry));
    }
  }

  public static byte[] encodeReconnect(String name, UUID identity, StateResponse.LeadershipState leadership) {
    try {
      try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
        try (DataOutputStream dout = new DataOutputStream(bout)) {

          MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

          if (leadership == null) {
            dout.writeInt(1);
            {
              byte[] joinMessage = codec.encodeMessage(new JoinRequest(name, identity));
              dout.writeInt(joinMessage.length);
              dout.write(joinMessage);
            }
          } else {
            dout.writeInt(2);
            {
              byte[] joinMessage = codec.encodeMessage(new JoinRequest(name, identity));
              dout.writeInt(joinMessage.length);
              dout.write(joinMessage);
            }

            {
              byte[] leadershipMessage = codec.encodeMessage(new LeadershipRequest(between(now(), leadership.getExpiry())));
              dout.writeInt(leadershipMessage.length);
              dout.write(leadershipMessage);
            }
          }
        }
        return bout.toByteArray();
      }
    } catch (MessageCodecException | IOException e) {
      throw new AssertionError(e);
    }
  }

  public static Tuple<JoinRequest, LeadershipRequest> decodeReconnect(byte[] reconnectData) throws MessageCodecException, IOException {
    try (ByteArrayInputStream bin = new ByteArrayInputStream(reconnectData)) {
      try (DataInputStream din = new DataInputStream(bin)) {

        MessageCodec<CoordinationMessage, StateResponse> codec = CoordinatorMessaging.codec();

        int messages = din.readInt();
        switch (messages) {
          case 1:
            return Tuple.of((JoinRequest) codec.decodeMessage(parseMessageData(din)), null);
          case 2:
            JoinRequest joinRequest = (JoinRequest) codec.decodeMessage(parseMessageData(din));
            LeadershipRequest leadershipRequest = (LeadershipRequest) codec.decodeMessage(parseMessageData(din));
            return Tuple.of(joinRequest, leadershipRequest);
          default:
            throw new MessageCodecException("Too many encoded messages", null);
        }
      }
    }
  }

  private static byte[] parseMessageData(DataInput input) throws IOException {
    try {
      int messageSize = input.readInt();
      byte[] messageData = new byte[messageSize];
      input.readFully(messageData);
      return messageData;
    } catch (EOFException e) {
      return null;
    }
  }
}
