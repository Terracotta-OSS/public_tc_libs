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


import com.terracottatech.tools.command.Command;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.ArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.ArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class CommandRunnelCodec {

  private static final String CLUSTER_NAME_FIELD = "clusterName";
  private static final String COMMAND_TYPE_FIELD = "commandType";
  private static final String ARGS_FIELD = "args";

  // runnel command struct.
  private static final Struct COMMAND_STRUCT;

  static {
    // Create command struct to encode Command object
    COMMAND_STRUCT = StructBuilder.newStructBuilder()
        .string(CLUSTER_NAME_FIELD, 10)
        .enm(COMMAND_TYPE_FIELD, 20, Command.COMMAND_TYPE_ENUM_MAPPING)
        .strings(ARGS_FIELD, 30)
        .build();
  }

  public ByteBuffer encode(Command command) {
    // Create encoder
    StructEncoder<Void> commandStructEncoder = COMMAND_STRUCT.encoder();

    // Add Cluster name
    commandStructEncoder = commandStructEncoder.string(CLUSTER_NAME_FIELD, command.getClusterName());

    // Add Command Type
    commandStructEncoder = commandStructEncoder.enm(COMMAND_TYPE_FIELD, command.getCommandType());

    // Add Args
    ArrayEncoder<String, StructEncoder<Void>> argsArrayEncoder = commandStructEncoder.strings(ARGS_FIELD);
    for (String arg : command.getArgs()) {
      argsArrayEncoder.value(arg);
    }
    commandStructEncoder = argsArrayEncoder.end();

    // Encode all the added values and return the byte buffer.
    return commandStructEncoder.encode();
  }

  public Command decode(ByteBuffer byteBuffer) {
    // Create the decoder
    StructDecoder<Void> commandStructDecoder = COMMAND_STRUCT.decoder(byteBuffer);

    // Extract Cluster name field
    String clusterName = commandStructDecoder.string(CLUSTER_NAME_FIELD);

    // Extract command type field
    Command.COMMAND_TYPE commandType = commandStructDecoder.<Command.COMMAND_TYPE>enm(COMMAND_TYPE_FIELD).get();

    // Extract args field
    ArrayDecoder<String, StructDecoder<Void>> argsArrayDecoder = commandStructDecoder.strings(ARGS_FIELD);
    List<String> args = new ArrayList<String>();
    for (int i = 0; i < argsArrayDecoder.length(); i++) {
      args.add(argsArrayDecoder.value());
    }
    argsArrayDecoder.end();

    return new Command(clusterName, commandType, args.toArray(new String[]{}));
  }
}
