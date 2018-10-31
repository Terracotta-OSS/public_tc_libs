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

import com.terracottatech.security.audit.AuditLogger;
import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.config.ClusterConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.monitoring.PlatformService;

public class TopologyPassiveEntity extends TopologyServerEntity implements PassiveServerEntity<Command, CommandResult> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyPassiveEntity.class);

  private final PlatformService platformService;
  private final AuditLogger auditLogger;

  public TopologyPassiveEntity(ClusterConfiguration configuration, AuditLogger auditLogger, PlatformService platformService) {
    super(configuration);
    this.platformService = platformService;
    this.auditLogger = auditLogger;
  }

  @SuppressFBWarnings("DM_EXIT")
  public void invokePassive(InvokeContext context, Command command) {
    switch (command.getCommandType()) {
      case DUMP_STATE_CLUSTER:
        auditLogger.info("Dump invoked:- ClientId: {}", context.getClientSource().toLong());
        LOGGER.debug("COMMAND_TYPE: DUMP_STATE_CLUSTER");
        if (checkClusterName(command) == null) {
          platformService.dumpPlatformState();
        }
        break;
      case STOPS_CLUSTER:
        auditLogger.info("Stop invoked:- ClientId: {}", context.getClientSource().toLong());
        LOGGER.debug("COMMAND_TYPE: STOPS_CLUSTER");
        if (checkClusterName(command) == null) {
          platformService.stopPlatform();
        }
        break;
      default:
        LOGGER.error("Command failed to execute on passive : {}, unknown COMMAND_TYPE", command.getCommandType());
    }
  }

  @Override
  public void startSyncEntity() {
    //no op
  }

  @Override
  public void endSyncEntity() {
    //no op
  }

  @Override
  public void startSyncConcurrencyKey(int i) {
    throw new UnsupportedOperationException("Passive Synchronization is not supported for TopologyEntity.");
  }

  @Override
  public void endSyncConcurrencyKey(int i) {
    throw new UnsupportedOperationException("Passive Synchronization is not supported for TopologyEntity.");
  }
}
