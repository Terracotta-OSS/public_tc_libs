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

import com.terracottatech.tools.command.runnelcodecs.CommandResultRunnelCodec;
import com.terracottatech.tools.command.runnelcodecs.CommandRunnelCodec;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

import java.nio.ByteBuffer;

public class CommandCodec implements MessageCodec<Command, CommandResult> {

  public static final CommandCodec MESSAGE_CODEC = new CommandCodec();

  private static final CommandRunnelCodec COMMAND_RUNNEL_CODEC = new CommandRunnelCodec();

  private static final CommandResultRunnelCodec COMMAND_RESULT_RUNNEL_CODEC  = new CommandResultRunnelCodec();

  private CommandCodec() {
  }

  @Override
  public byte[] encodeMessage(Command command) throws MessageCodecException {
    return COMMAND_RUNNEL_CODEC.encode(command).array();
  }

  @Override
  public Command decodeMessage(byte[] bytes) throws MessageCodecException {
    return COMMAND_RUNNEL_CODEC.decode(ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] encodeResponse(CommandResult commandResult) throws MessageCodecException {
    return COMMAND_RESULT_RUNNEL_CODEC.encode(commandResult).array();
  }

  @Override
  public CommandResult decodeResponse(byte[] bytes) throws MessageCodecException {
    return COMMAND_RESULT_RUNNEL_CODEC.decode(ByteBuffer.wrap(bytes));
  }
}
