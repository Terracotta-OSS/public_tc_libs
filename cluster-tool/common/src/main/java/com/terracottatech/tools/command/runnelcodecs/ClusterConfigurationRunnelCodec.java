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
package com.terracottatech.tools.command.runnelcodecs;


import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;

public class ClusterConfigurationRunnelCodec {

  private static final ClusterConfigurationRunnelCodec CLUSTER_CONFIGURATION_RUNNEL_CODEC = new ClusterConfigurationRunnelCodec();

  public static final Struct CLUSTER_CONFIGURATION_STRUCT;

  private static final String TYPE_FIELD = "type";
  private static final String VALUE_FIELD = "value";

  private static final String NAME_FIELD = "name";
  private static final String HOST_FIELD = "host";
  private static final String PORT_FIELD = "port";

  private static final String CLUSTER_NAME_FIELD = "clusterName";
  private static final String STRIPES_FIELD = "stripes";

  private static final String CONFIGS_FIELD = "configs";
  private static final String SERVERS_FIELD = "servers";

  private static final Struct CONFIG_STRUCT;
  private static final Struct SERVER_STRUCT;
  private static final Struct STRIPE_STRUCT;

  static {
    CONFIG_STRUCT = StructBuilder.newStructBuilder()
        .string(TYPE_FIELD, 10)
        .string(NAME_FIELD, 20)
        .string(VALUE_FIELD, 30)
        .build();

    SERVER_STRUCT = StructBuilder.newStructBuilder()
        .string(NAME_FIELD, 10)
        .string(HOST_FIELD, 20)
        .int32(PORT_FIELD, 30)
        .build();

    STRIPE_STRUCT = StructBuilder.newStructBuilder()
        .structs(CONFIGS_FIELD, 10, CONFIG_STRUCT)
        .structs(SERVERS_FIELD, 20, SERVER_STRUCT)
        .build();

    CLUSTER_CONFIGURATION_STRUCT = StructBuilder.newStructBuilder()
        .string(CLUSTER_NAME_FIELD, 10)
        .structs(ClusterConfigurationRunnelCodec.STRIPES_FIELD, 20, STRIPE_STRUCT)
        .build();
  }

  private ClusterConfigurationRunnelCodec() {

  }

  public static byte[] encode(ClusterConfiguration clusterConfiguration) {
    StructEncoder<Void> clusterConfigurationStructEncoder = CLUSTER_CONFIGURATION_STRUCT.encoder();
    encode(clusterConfigurationStructEncoder, clusterConfiguration);
    return clusterConfigurationStructEncoder.encode().array();
  }

  public static ClusterConfiguration decode(byte[] bytes) {
    return decode(CLUSTER_CONFIGURATION_STRUCT.decoder(ByteBuffer.wrap(bytes)));
  }

  public static ClusterConfiguration decode(StructDecoder<?> decoder) {
    return CLUSTER_CONFIGURATION_RUNNEL_CODEC.decodeClusterConfiguration(decoder);
  }

  public static void encode(StructEncoder<?> encoder, ClusterConfiguration clusterConfiguration) {
    CLUSTER_CONFIGURATION_RUNNEL_CODEC.encodeClusterConfiguration(encoder, clusterConfiguration);
  }

  private void encodeClusterConfiguration(StructEncoder<?> encoder, ClusterConfiguration clusterConfiguration) {
    encoder.string(CLUSTER_NAME_FIELD, clusterConfiguration.getClusterName());
    encoder.structs(ClusterConfigurationRunnelCodec.STRIPES_FIELD, clusterConfiguration.getCluster().getStripes(), (StructEncoderFunction<Stripe>) this::encodeStripe);
  }

  private ClusterConfiguration decodeClusterConfiguration(StructDecoder<?> decoder) {
    String clusterName = decoder.string(CLUSTER_NAME_FIELD);

    List<Stripe> stripes = new ArrayList<>();

    StructArrayDecoder<?> stripeStructArrayDecoder = decoder.structs(ClusterConfigurationRunnelCodec.STRIPES_FIELD);
    while (stripeStructArrayDecoder.hasNext()) {
      stripes.add(decodeStripe(stripeStructArrayDecoder.next()));
    }
    stripeStructArrayDecoder.end();
    return new ClusterConfiguration(clusterName, new Cluster(stripes));
  }


  private void encodeStripe(StructEncoder<?> encoder, Stripe stripe) {
    encoder.structs(CONFIGS_FIELD, stripe.getConfigs(), (StructEncoderFunction<Stripe.Config<?>>) this::encodeConfig);
    encoder.structs(SERVERS_FIELD, stripe.getServers(), (StructEncoderFunction<Server>) this::encodeServer);
  }

  private Stripe decodeStripe(StructDecoder<?> decoder) {
    List<Stripe.Config<?>> configs = new ArrayList<>();
    List<Server> servers = new ArrayList<>();

    // Decoding Configs
    StructArrayDecoder<?> configsStructArrayDecoder = decoder.structs(CONFIGS_FIELD);
    while (configsStructArrayDecoder.hasNext()) {
      configs.add(decodeConfig(configsStructArrayDecoder.next()));
    }
    configsStructArrayDecoder.end();

    // Decoding servers
    StructArrayDecoder<?> serverStructArrayDecoder = decoder.structs(SERVERS_FIELD);
    while (serverStructArrayDecoder.hasNext()) {
      servers.add(decodeServer(serverStructArrayDecoder.next()));
    }
    serverStructArrayDecoder.end();

    return new Stripe(servers, configs.toArray(new Stripe.Config<?>[]{}));
  }


  private void encodeServer(StructEncoder<?> encoder, Server server) {
    encoder.string(NAME_FIELD, server.getName());
    encoder.string(HOST_FIELD, server.getHost());
    encoder.int32(PORT_FIELD, server.getPort());
  }

  private Server decodeServer(StructDecoder<?> decoder) {
    String name = decoder.string(NAME_FIELD);
    String host = decoder.string(HOST_FIELD);
    int port = decoder.int32(PORT_FIELD);

    return new Server(name, host, port);
  }


  private void encodeConfig(StructEncoder<?> encoder, Stripe.Config<?> config) {
    encoder.string(TYPE_FIELD, config.getType().getName());
    encoder.string(NAME_FIELD, config.getName());
    encoder.string(VALUE_FIELD, config.getValue().toString());
  }

  private Stripe.Config<?> decodeConfig(StructDecoder<?> decoder) {
    String type = decoder.string(TYPE_FIELD);
    String name = decoder.string(NAME_FIELD);
    String value = decoder.string(VALUE_FIELD);

    if (type.equals(OFFHEAP.getName())) {
      return new Stripe.Config<>(OFFHEAP, name, Long.parseLong(value));
    }

    return new Stripe.Config<>(Stripe.ConfigType.of(type), name, value);
  }
}
