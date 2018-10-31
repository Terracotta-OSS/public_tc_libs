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
package com.terracottatech.tools.validation;

import org.junit.Test;

import com.terracottatech.tools.config.Cluster;

public class ConfigurationValidator_ValidateStripeAgainstClusterTest extends BaseConfigurationValidatorTest {

  @Test
  public void testIdenticalConfigsAndHosts() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("startup/tc-config1.xml"),
        readContent("startup/tc-config2.xml"));
    Cluster serverStartup = configurationParser.parseConfigurations(readContent("startup/tc-config1.xml"));

    validator.validateStripeAgainstCluster(serverStartup.getStripes().get(0), cluster);
  }

  @Test
  public void testIdenticalConfigsAndHostsNotFirst() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("startup/tc-config2.xml"),
        readContent("startup/tc-config1.xml"));
    Cluster serverStartup = configurationParser.parseConfigurations(readContent("startup/tc-config1.xml"));

    validator.validateStripeAgainstCluster(serverStartup.getStripes().get(0), cluster);
  }

  @Test
  public void testIdenticalConfigsAndMismatchedHosts() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("startup/tc-config1.xml"),
        readContent("startup/tc-config2.xml"));
    Cluster serverStartup = configurationParser.parseConfigurations(readContent("invalid/tc-config-busted.xml"));

    testValidationWithException(serverStartup, cluster, "no match found");
  }

  @Test
  public void testIdenticalConfigsAndMismatchedHostsNotFirst() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("startup/tc-config2.xml"),
        readContent("startup/tc-config1.xml"));
    Cluster serverStartup = configurationParser.parseConfigurations(readContent("invalid/tc-config-busted.xml"));

    testValidationWithException(serverStartup, cluster, "no match found");
  }

  @Test
  public void testStripeWithSubstitutionParameter() {
    Cluster cluster = configurationParser.parseConfigurations(readContent("config-types/tc-config2.xml"));
    Cluster serverStartup = configurationParser.parseConfigurations(readContent("minimal/tc-config-substitutable-name.xml"));
    validator.validateStripeAgainstCluster(serverStartup.getStripes().get(0), cluster);
  }

  @Test
  public void testConfigTypes_NewConfigHasMoreTypes() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config2.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config1.xml"));

    testValidationWithException(newCluster, configuredCluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testConfigTypes_NewConfigHasLessTypes() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config2.xml"));

    testValidationWithException(newCluster, configuredCluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testOffHeapConfigOnStartup_NewSizeGreater_SameNumberOfElements() {
    Cluster largeCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));
    Cluster smallCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config4.xml"));

    testValidationWithException(largeCluster, smallCluster, "Provided: primary-server-resource: 768G, but previously known: primary-server-resource: 76G");
  }

  @Test
  public void testOffHeapConfigOnStartup_NewSizeLesser_SameNumberOfElements() {
    Cluster largeCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));
    Cluster smallCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config4.xml"));

    testValidationWithException(smallCluster, largeCluster, "Provided: primary-server-resource: 76G, but previously known: primary-server-resource: 768G");
  }

  @Test
  public void testOffHeapConfigOnStartup_NewSizeEqual_SameNumberOfElements() {
    Cluster largeCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));
    Cluster smallCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));

    validator.validateStripeAgainstCluster(smallCluster.getStripes().get(0), largeCluster);
  }

  @Test
  public void testOffHeapConfig_NewConfigHasMoreElements_KnownElementSizesEqual() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config5.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));

    testValidationWithException(newCluster, configuredCluster, OFFHEAP_MISMATCH);
  }

  @Test
  public void testDataRoots_SameNumberOfElements_ConfigsMatch() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config9.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config9.xml"));

    validator.validateStripeAgainstCluster(newCluster.getStripes().get(0), configuredCluster);
  }

  @Test
  public void testDataRoots_SameNumberOfElements_ConfigMismatch() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config9.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config10.xml"));

    testValidationWithException(newCluster, configuredCluster, DATA_DIRECTORIES_MISMATCH);
  }

  @Test
  public void testDataRoots_NewConfigHasMoreElements_KnownElementsMatch() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config10.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config8.xml"));

    testValidationWithException(newCluster, configuredCluster, DATA_DIRECTORIES_MISMATCH);
  }

  @Test
  public void testDataRoots_NewConfigHasMoreElements_KnownElementsMisMatch() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config9.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config7.xml"));

    testValidationWithException(newCluster, configuredCluster, DATA_DIRECTORIES_MISMATCH);
  }

  @Test
  public void testDataRoots_NewConfigHasLessElements() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config8.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("data-roots/tc-config10.xml"));

    testValidationWithException(newCluster, configuredCluster, DATA_DIRECTORIES_MISMATCH);
  }

  @Test
  public void testConsistentFailoverPriorityAvailability_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));

    validator.validateStripeAgainstCluster(newCluster.getStripes().get(0), configuredCluster);
  }

  @Test
  public void testConsistentFailoverPriorityConsistency_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));

    validator.validateStripeAgainstCluster(newCluster.getStripes().get(0), configuredCluster);
  }

  @Test
  public void testConsistentFailoverPriorityConsistencyVoterCount_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-1.xml"));

    validator.validateStripeAgainstCluster(newCluster.getStripes().get(0), configuredCluster);
  }

  @Test
  public void testInconsistentFailoverPriority_Failure() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));

    testValidationWithException(newCluster, configuredCluster, FAILOVER_PRIORITY_MISMATCH);
  }

  @Test
  public void testInconsistentFailoverPriorityVoterCount_Failure() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-voter-1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-voter-3.xml"));

    testValidationWithException(newCluster, configuredCluster, FAILOVER_PRIORITY_MISMATCH);
  }

  private void testValidationWithException(Cluster newCluster, Cluster configuredCluster, String exceptionString) {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(exceptionString);
    validator.validateStripeAgainstCluster(newCluster.getStripes().get(0), configuredCluster);
  }
}
