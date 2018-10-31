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

import com.terracottatech.tools.config.ClusterConfiguration;
import org.terracotta.entity.EntityResponse;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;

import java.io.Serializable;

public class CommandResult implements Serializable, EntityResponse {
  private static final long serialVersionUID = 2504245444188771202L;

  // Command Result Type enum mapping for runnel codec.
  public static final EnumMapping<COMMAND_RESULT_TYPE> COMMAND_RESULT_TYPE_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(CommandResult.COMMAND_RESULT_TYPE.class)
      .mapping(COMMAND_RESULT_TYPE.SUCCESS, 10)
      .mapping(COMMAND_RESULT_TYPE.FAILURE, 20)
      .mapping(COMMAND_RESULT_TYPE.CONFIG, 30)
      .build();

  public enum COMMAND_RESULT_TYPE {
    SUCCESS,
    FAILURE,
    CONFIG
  }
  private final COMMAND_RESULT_TYPE  commandResultType;
  private final String message;

  public CommandResult(COMMAND_RESULT_TYPE commandResultType, String message) {
    this.commandResultType = commandResultType;
    this.message = message;
  }


  public COMMAND_RESULT_TYPE getCommandResultType() {
    return commandResultType;
  }

  public String getMessage() {
    return message;
  }

  @SuppressWarnings("serial")
  public static final class ConfigResult extends CommandResult {

    private final ClusterConfiguration clusterConfiguration;

    public ConfigResult(ClusterConfiguration clusterConfiguration) {
      super(COMMAND_RESULT_TYPE.CONFIG, "");
      this.clusterConfiguration = clusterConfiguration;
    }

    public ClusterConfiguration getClusterConfiguration() {
      return clusterConfiguration;
    }

    @Override
    public String getMessage() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}
