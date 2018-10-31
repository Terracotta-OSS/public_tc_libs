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

import org.junit.Test;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EnterpriseTCVoterMainTest {
  @Test
  public void testMultipleServerOptArgs() throws Exception {
    AtomicInteger stripeCount = new AtomicInteger(0);
    EnterpriseTCVoterMain voterMain = new EnterpriseTCVoterMain() {
      @Override
      protected void startVoter(Optional<Properties> connectionProps, String... hostPorts) {
        stripeCount.incrementAndGet();
      }
    };

    String[] args = new String[] {"-s", "foo:1234", "-s", "bar:2345"};
    voterMain.processArgs(args);
    assertThat(stripeCount.get(), is(2));
  }

  @Test
  public void testMultipleConfigFileOptArgs() throws Exception {
    AtomicInteger stripeCount = new AtomicInteger(0);
    EnterpriseTCVoterMain voterMain = new EnterpriseTCVoterMain() {
      @Override
      protected void startVoter(Optional<Properties> connectionProps, String... hostPorts) {
        stripeCount.incrementAndGet();
      }
    };

    String[] args = new String[] {"-f", "src/test/resources/tc-config-1.xml", "-f", "src/test/resources/tc-config-2.xml"};
    voterMain.processArgs(args);
    assertThat(stripeCount.get(), is(2));
  }

  @Test
  public void testSecurityRootDirectoryArg() throws Exception {
    String securityDirectoryPath = "security/root/directory";
    EnterpriseTCVoterMain voterMain = new EnterpriseTCVoterMain() {
      @Override
      protected void startVoter(Optional<Properties> connectionProps, String... hostPorts) {
        assertThat(connectionProps.get().getProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY), is(securityDirectoryPath));
      }
    };

    String[] args = new String[] {"-srd", securityDirectoryPath, "-s", "foo:1234"};
    voterMain.processArgs(args);
  }

}