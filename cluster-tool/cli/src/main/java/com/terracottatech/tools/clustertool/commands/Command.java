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
package com.terracottatech.tools.clustertool.commands;


import com.beust.jcommander.JCommander;
import com.terracottatech.tools.clustertool.managers.CommandManager;

/**
 * Interface to be implemented by all cluster-tool commands.
 *
 * Command implementations should be registered in {@link CommandManager}.
 */
public interface Command {

  /**
   * Returns the name of the command that this class can handle.
   *
   * @return {@code String} containing the name of the command
   */
  String name();

  /**
   * Process this command, while the parameters are already injected into this instance.
   * @param  jCommander {@link JCommander} instance
   */
  void process(JCommander jCommander);

  /**
   * Returns the usage for the command.
   *
   * @return {@code String} containing the usage of the command
   */
  String usage();
}
