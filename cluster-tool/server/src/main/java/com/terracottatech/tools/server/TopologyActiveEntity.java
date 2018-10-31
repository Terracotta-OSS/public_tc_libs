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
package com.terracottatech.tools.server;

import com.tc.objectserver.core.api.GuardianContext;
import com.terracottatech.security.audit.AuditLogger;
import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.command.CommandResult.COMMAND_RESULT_TYPE;
import com.terracottatech.tools.config.ClusterConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.monitoring.PlatformService;

import java.util.Properties;

import static com.terracottatech.security.audit.AuditUtil.getClientIpAddress;
import static com.terracottatech.security.audit.AuditUtil.getUsername;

public class TopologyActiveEntity extends TopologyServerEntity implements ActiveServerEntity<Command, CommandResult> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyActiveEntity.class);

  private final PlatformService platformService;
  private final AuditLogger auditLogger;

  public TopologyActiveEntity(ClusterConfiguration configuration, AuditLogger auditLogger, PlatformService platformService) {
    super(configuration);
    this.platformService = platformService;
    this.auditLogger = auditLogger;
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    //no op
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    //no op
  }

  @SuppressFBWarnings("DM_EXIT")
  public CommandResult invokeActive(ActiveInvokeContext context, Command command) {
    switch (command.getCommandType()) {
      case FETCH_CONFIGURATION:
        LOGGER.debug("COMMAND_TYPE: FETCH_CONFIGURATION");
        return new CommandResult.ConfigResult(getClusterConfiguration());
      case DUMP_STATE_CLUSTER:
        LOGGER.debug("COMMAND_TYPE: DUMP_STATE_CLUSTER");
        audit(context, "Dump");

        CommandResult failure = checkClusterName(command);
        if (failure == null) {
          platformService.dumpPlatformState();
          break;
        } else {
          return failure;
        }
      case STOPS_CLUSTER:
        LOGGER.debug("COMMAND_TYPE: STOPS_CLUSTER");
        audit(context, "Stop");

        failure = checkClusterName(command);
        if (failure == null) {
          platformService.stopPlatform();
          break;
        } else {
          return failure;
        }
      default:
        LOGGER.error("COMMAND_TYPE: UNKNOWN");
        return new CommandResult(COMMAND_RESULT_TYPE.FAILURE, "Command failed to execute : " + command.getCommandType() +
            ", unknown command type");
    }
    return new CommandResult(COMMAND_RESULT_TYPE.SUCCESS, "Command successfully executed : " + command.getCommandType());
  }

  @Override
  public void loadExisting() {
    //no op
  }

  @Override
  public ReconnectHandler startReconnect() {
    return null;
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<Command> passiveSynchronizationChannel, int i) {
    throw new UnsupportedOperationException("Passive Synchronization is not supported for TopologyEntity.");
  }

  private void audit(ActiveInvokeContext context, String command) {
    Properties properties = GuardianContext.getCurrentChannelProperties();
    String username = getUsername(properties);
    long clientId = context.getClientSource().toLong();
    String clientIp = getClientIpAddress(properties);

    if (username == null) {
      auditLogger.info(command + " invoked:- IP: {}, ClientId: {}", clientIp, clientId);
    } else {
      auditLogger.info(command + " invoked:- IP: {}, User: {}, ClientId: {}", clientIp, username, clientId);
    }
  }
}
