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

import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.config.ClusterConfiguration;
import org.terracotta.connection.entity.Entity;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvocationBuilder;
import org.terracotta.entity.InvokeFuture;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static com.terracottatech.tools.command.CommandResult.COMMAND_RESULT_TYPE.CONFIG;
import static com.terracottatech.tools.command.CommandResult.COMMAND_RESULT_TYPE.FAILURE;
import static com.terracottatech.tools.command.CommandResult.ConfigResult;

public class TopologyEntity implements Entity {
  private static final int OP_TIMEOUT_MILLIS = 60000;

  private final EntityClientEndpoint<Command, CommandResult> entityClientEndpoint;

  public TopologyEntity(EntityClientEndpoint<Command, CommandResult> entityClientEndpoint) {
    this.entityClientEndpoint = entityClientEndpoint;
    entityClientEndpoint.setDelegate(new EndpointDelegate<CommandResult>() {
      public void handleMessage(CommandResult messageFromServer) {
        //no op
      }

      public byte[] createExtendedReconnectData() {
        return new byte[0];
      }

      public void didDisconnectUnexpectedly() {
      }
    });
  }

  public ClusterConfiguration getClusterConfiguration() {
    try {
      CommandResult commandResult = entityClientEndpoint.beginInvoke().
              message(new Command(Command.COMMAND_TYPE.FETCH_CONFIGURATION)).invoke().get();
      if (commandResult.getCommandResultType().equals(CONFIG)) {
        return ((ConfigResult) commandResult).getClusterConfiguration();
      } else {
        throw new RuntimeException("Failed to retrieve ClusterConfiguration : " + commandResult.getMessage());
      }
    } catch (Exception e) {
      throw new ClusterToolException(StatusCode.INTERNAL_ERROR, e);
    }
  }

  public void close() {
    entityClientEndpoint.close();
  }

  public void release() {
    entityClientEndpoint.release();
  }

  public CommandResult executeCommand(Command command) throws InterruptedException, TimeoutException {
    return executeCommand(command, false, false);
  }

  public CommandResult executeCommand(Command command, boolean ignoreFuture, boolean waitForAckReceived) throws InterruptedException, TimeoutException {
    try {
      InvocationBuilder<Command, CommandResult> invocationBuilder = entityClientEndpoint.beginInvoke().message(command);
      if (waitForAckReceived) {
        invocationBuilder = invocationBuilder.ackReceived();
      }

      long startTime = System.currentTimeMillis();
      InvokeFuture<CommandResult> invokeFuture = invocationBuilder.invokeWithTimeout(OP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      long remainingTime = OP_TIMEOUT_MILLIS - (System.currentTimeMillis() - startTime);
      if (ignoreFuture) {
        return null;
      } else {
        return invokeFuture.getWithTimeout(remainingTime, TimeUnit.MILLISECONDS);
      }
    } catch (EntityException e) {
      return new CommandResult(FAILURE, "Entity failure : " + e.getMessage());
    } catch (MessageCodecException e) {
      return new CommandResult(FAILURE, "Message Codec failure : " + e.getMessage());
    }
  }
}
