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
package com.terracottatech.tools.clustertool.parser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.Strings;
import com.beust.jcommander.WrappedParameter;
import com.beust.jcommander.internal.Lists;
import com.terracottatech.tools.clustertool.commands.Command;
import com.terracottatech.tools.clustertool.managers.CommandManager;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Class containing overridden usage methods from JCommander.
 */
public class CustomJCommander extends JCommander {
  private final CommandManager commandManager;

  public CustomJCommander(Command command, CommandManager commandManager) {
    super(command);
    this.commandManager = commandManager;
  }

  @Override
  public void usage(String commandName, StringBuilder out, String indent) {
    String description = getCommandDescription(commandName);
    JCommander jc = getCommands().get(commandName);
    if (description != null) {
      out.append(indent).append(description).append("\n");
    }
    appendUsage(commandManager.getCommand(commandName), out, indent);
    appendOptions(jc, out, indent);
  }

  @Override
  public void usage(StringBuilder out, String indent) {
    boolean hasCommands = !getCommands().isEmpty();
    String programName = "cluster-tool";
    out.append(indent).append("Usage: ").append(programName).append(" [options]");
    if (hasCommands) {
      out.append(indent).append(" [command] [command-options]");
    }

    if (getMainParameter() != null) {
      out.append(" ").append(getMainParameter().getDescription());
    }
    out.append("\n");
    appendOptions(this, out, indent);

    // Show Commands
    if (hasCommands) {
      out.append("\n\nCommands:\n");
      for (Map.Entry<String, JCommander> command : getCommands().entrySet()) {
        Object arg = command.getValue().getObjects().get(0);
        Parameters p = arg.getClass().getAnnotation(Parameters.class);
        String name = command.getKey();
        if (p == null || !p.hidden()) {
          String description = getCommandDescription(name);
          out.append(indent).append("    ").append(name).append("      ").append(description).append("\n");
          appendUsage(commandManager.getCommand(name), out, indent + "    ");

          // Options for this command
          JCommander jc = command.getValue();
          appendOptions(jc, out, "    ");
          out.append("\n");
        }
      }
    }
  }

  private void appendUsage(Command command, StringBuilder out, String indent) {
    out.append(indent).append("Usage:\n");
    out.append(indent).append("    ").append(command.usage().replaceAll("\n", "\n    " + indent)).append("\n");
  }

  private void appendOptions(JCommander jCommander, StringBuilder out, String indent) {
    // Align the descriptions at the "longestName" column
    int longestName = 0;
    List<ParameterDescription> sorted = Lists.newArrayList();
    for (ParameterDescription pd : jCommander.getParameters()) {
      if (!pd.getParameter().hidden()) {
        sorted.add(pd);
        int length = pd.getNames().length() + 2;
        if (length > longestName) {
          longestName = length;
        }
      }
    }

    // Sort the options
    sorted.sort(Comparator.comparing(ParameterDescription::getLongestName));

    // Display all the names and descriptions
    if (sorted.size() > 0) {
      out.append(indent).append("Options:\n");
    }

    for (ParameterDescription pd : sorted) {
      WrappedParameter parameter = pd.getParameter();
      out.append(indent).append("    ").append(pd.getNames()).append(parameter.required() ? " (required)" : "").append("\n");
      out.append(indent).append("        ").append(pd.getDescription());
      out.append("\n");
    }
  }
}
