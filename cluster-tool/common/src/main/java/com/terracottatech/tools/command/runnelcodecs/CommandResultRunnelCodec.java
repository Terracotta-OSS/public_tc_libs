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


import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.config.ClusterConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;
import org.terracotta.runnel.encoding.StructEncoderFunction;

import java.nio.ByteBuffer;

public class CommandResultRunnelCodec {

  private static final String COMMAND_RESULT_TYPE_FIELD = "commandResultType";
  private static final String MESSAGE_FIELD = "message";
  private static final String CLUSTER_CONFIGURATION_FIELD = "clusterConfiguration";

  private static final Struct COMMAND_RESULT_STRUCT;

  static {
    COMMAND_RESULT_STRUCT = StructBuilder.newStructBuilder()
        .enm(COMMAND_RESULT_TYPE_FIELD, 10, CommandResult.COMMAND_RESULT_TYPE_ENUM_MAPPING)
        .string(MESSAGE_FIELD, 20)
        .struct(CLUSTER_CONFIGURATION_FIELD, 30, ClusterConfigurationRunnelCodec.CLUSTER_CONFIGURATION_STRUCT)
        .build();
  }

  public ByteBuffer encode(CommandResult commandResult) {
    StructEncoder<Void> commandResultStructEncoder = COMMAND_RESULT_STRUCT.encoder();

    commandResultStructEncoder = commandResultStructEncoder.enm(COMMAND_RESULT_TYPE_FIELD, commandResult.getCommandResultType());

    // Only add cluster configuration if an instance of ConfigResult, otherwise add the message
    if (commandResult instanceof CommandResult.ConfigResult) {
      commandResultStructEncoder = commandResultStructEncoder.struct(CLUSTER_CONFIGURATION_FIELD, ((CommandResult.ConfigResult) commandResult).getClusterConfiguration(),
          new StructEncoderFunction<ClusterConfiguration>() {
            @Override
            public void encode(StructEncoder<?> encoder, ClusterConfiguration clusterConfiguration) {
              ClusterConfigurationRunnelCodec.encode(encoder, clusterConfiguration);
            }
          });
    } else {
      commandResultStructEncoder = commandResultStructEncoder.string(MESSAGE_FIELD, commandResult.getMessage());
    }

    // Encode all the added values and return the buffer.
    return commandResultStructEncoder.encode();
  }


  public CommandResult decode(ByteBuffer byteBuffer) {
    StructDecoder<Void> commandResultStructDecoder = COMMAND_RESULT_STRUCT.decoder(byteBuffer);

    CommandResult.COMMAND_RESULT_TYPE commandResultType = commandResultStructDecoder.<CommandResult.COMMAND_RESULT_TYPE>enm(COMMAND_RESULT_TYPE_FIELD).get();
    String message = commandResultStructDecoder.string(MESSAGE_FIELD);

    if (message != null) {
      return new CommandResult(commandResultType, message);
    }

    ClusterConfiguration clusterConfiguration = ClusterConfigurationRunnelCodec.decode(commandResultStructDecoder.struct(CLUSTER_CONFIGURATION_FIELD));
    return new CommandResult.ConfigResult(clusterConfiguration);
  }
}
