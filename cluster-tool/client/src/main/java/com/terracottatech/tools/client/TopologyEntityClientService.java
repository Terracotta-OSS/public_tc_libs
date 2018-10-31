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
package com.terracottatech.tools.client;

import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandCodec;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Stripe;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

import java.util.ArrayList;

import static com.terracottatech.tools.command.runnelcodecs.ClusterConfigurationRunnelCodec.decode;
import static com.terracottatech.tools.command.runnelcodecs.ClusterConfigurationRunnelCodec.encode;

public class TopologyEntityClientService implements EntityClientService<TopologyEntity, ClusterConfiguration, Command, CommandResult, Void> {

  @Override
  public boolean handlesEntityType(Class<TopologyEntity> cls) {
    return cls.equals(TopologyEntity.class);
  }

  public byte[] serializeConfiguration(ClusterConfiguration configuration) {
    return encode(configuration);
  }

  public ClusterConfiguration deserializeConfiguration(byte[] configuration) {
    if (configuration.length == 0) {
      return new ClusterConfiguration(null, new ArrayList<>());
    }
    return decode(configuration);
  }

  public TopologyEntity create(EntityClientEndpoint<Command, CommandResult> endpoint, Void empty) {
    return new TopologyEntity(endpoint);
  }

  public MessageCodec<Command, CommandResult> getMessageCodec() {
    return CommandCodec.MESSAGE_CODEC;
  }
}
