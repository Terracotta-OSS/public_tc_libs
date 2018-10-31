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
package com.terracottatech.voter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.terracotta.voter.TCVoter;
import org.terracotta.voter.TCVoterMain;

import com.tc.config.schema.setup.ConfigurationSetupException;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

import static com.terracottatech.connection.EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY;

public class EnterpriseTCVoterMain extends TCVoterMain {

  private static final String SECURITY_ROOT_DIRECTORY_OPTION = "srd";

  @Override
  protected void processServerArg(Optional<Properties> connectionProps, String[] stripes) throws ConfigurationSetupException {
    for (String stripe : stripes) {
      super.processServerArg(connectionProps, new String[] {stripe});
    }
  }

  @Override
  protected void processConfigFileArg(Optional<Properties> connectionProps, String[] stripes) throws ConfigurationSetupException {
    for (String stripe : stripes) {
      super.processConfigFileArg(connectionProps, new String[] {stripe});
    }
  }

  @Override
  protected Optional<Properties> getConnectionProperties(CommandLine commandLine) {
    Properties properties = super.getConnectionProperties(commandLine).orElse(new Properties());
    if (commandLine.hasOption(SECURITY_ROOT_DIRECTORY_OPTION)) {
      properties.setProperty(SECURITY_ROOT_DIRECTORY, commandLine.getOptionValue(SECURITY_ROOT_DIRECTORY_OPTION));
    }
    return Optional.of(properties);
  }

  @Override
  protected TCVoter getVoter(Optional<Properties> connectionProps) {
    String securityRootDirectory = connectionProps.orElse(new Properties()).getProperty(SECURITY_ROOT_DIRECTORY);
    if (securityRootDirectory != null) {
      return new EnterpriseTCVoterImpl(Paths.get(securityRootDirectory));
    }
    return new EnterpriseTCVoterImpl();
  }

  @Override
  protected Options voterOptions() {
    return super.voterOptions()
        .addOption(Option.builder(SECURITY_ROOT_DIRECTORY_OPTION).desc("Security root directory").hasArg().argName("security root directory path").build());
  }

  public static void main(String[] args) throws ConfigurationSetupException, ParseException {
    EnterpriseTCVoterMain voterMain = new EnterpriseTCVoterMain();
    voterMain.processArgs(args);
  }
}
