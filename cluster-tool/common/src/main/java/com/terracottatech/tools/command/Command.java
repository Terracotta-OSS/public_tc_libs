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
package com.terracottatech.tools.command;

import org.terracotta.entity.EntityMessage;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Command implements Serializable, EntityMessage {
  private static final long serialVersionUID = -4643198497781460770L;

  // Command Type enum mapping for runnel codec.
  public static final EnumMapping<COMMAND_TYPE> COMMAND_TYPE_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(Command.COMMAND_TYPE.class)
      .mapping(COMMAND_TYPE.FETCH_CONFIGURATION, 10)
      .mapping(COMMAND_TYPE.STOPS_CLUSTER, 20)
      .mapping(COMMAND_TYPE.DUMP_STATE_CLUSTER, 30)
      .build();

  public enum COMMAND_TYPE {
    FETCH_CONFIGURATION,
    STOPS_CLUSTER,
    DUMP_STATE_CLUSTER,
  }
  private final String clusterName;
  private final List<String> args;
  private final COMMAND_TYPE commandType;

  public Command(String clusterName, COMMAND_TYPE commandType, String... args) {
    this.clusterName = clusterName;
    this.commandType = commandType;
    this.args = Collections.unmodifiableList(Arrays.asList(args));
  }

  public Command(COMMAND_TYPE commandType, String... args) {
    this(null, commandType, args);
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public List<String> getArgs() {
    return this.args;
  }

  public COMMAND_TYPE getCommandType() {
    return commandType;
  }
}
