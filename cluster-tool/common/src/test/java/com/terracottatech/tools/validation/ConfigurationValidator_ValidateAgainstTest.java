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

import com.terracottatech.tools.config.Cluster;

import org.junit.Test;

// Tests for ConfigurationValidator::validateAgainst
public class ConfigurationValidator_ValidateAgainstTest extends BaseConfigurationValidatorTest {

  @Test
  public void testConfigTypes_NewConfigHasMoreTypes() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config3.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config2.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testConfigTypes_NewConfigHasLessTypes() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("config-types/tc-config2.xml"));

    testValidationWithException(newCluster, configuredCluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testOffHeapConfig_NewSizeGreater_SameNumberOfElements() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config4.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testOffHeapConfig_NewSizeSmaller_SameNumberOfElements() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config4.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));

    testValidationWithException(newCluster, configuredCluster, "Provided: primary-server-resource: 76G, but previously known: primary-server-resource: 768G");
  }

  @Test
  public void testOffHeapConfig_NewConfigHasMoreElements_KnownElementSizesEqual() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config5.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testOffHeapConfig_NewConfigHasMoreElements_KnownNewElementIsSmaller() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config6.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));

    testValidationWithException(newCluster, configuredCluster, OFFHEAP_MISMATCH);
  }

  @Test
  public void testOffHeapConfig_NewConfigHasMoreElements_KnownNewElementIsLarger() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config5.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config4.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testOffHeapConfig_NewConfigHasLessElements() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config3.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("offheap/tc-config6.xml"));

    testValidationWithException(newCluster, configuredCluster, OFFHEAP_MISMATCH);
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

    validator.validateAgainst(newCluster, configuredCluster);
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
  public void testSameConfigs() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("config-types/tc-config2.xml"),
        readContent("data-roots/tc-config7.xml"));

    validator.validateAgainst(cluster, cluster);
  }

  @Test
  public void testConsistentConfigs() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("consistent/tc-config12.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("consistent/tc-config11.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testNewSecurityConfigsHasMore() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist-cert_auth.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testNewSecurityConfigsHasLess() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist-cert_auth.xml"));

    testValidationWithException(newCluster, configuredCluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testConsistentFailoverPriorityAvailability_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testConsistentFailoverPriorityConsistency_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testConsistentFailoverPriorityConsistencyVoterCount_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-1.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testInconsistentFailoverPriority_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testInconsistentFailoverPriorityVoterCount_Success() {
    Cluster newCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-voter-1.xml"));
    Cluster configuredCluster = configurationParser.parseConfigurations(readContent("failover-priority/tc-config-consistency-voter-3.xml"));

    validator.validateAgainst(newCluster, configuredCluster);
  }

  @Test
  public void testInConsistentFailoverPriorityAcrossStripe_Failure() {
    Cluster configuredCluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-availability.xml"),
        readContent("failover-priority/tc-config-availability-1.xml"));

    Cluster newCluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-availability.xml"),
        readContent("failover-priority/tc-config-consistency.xml"));

    testValidationWithException(newCluster, configuredCluster, FAILOVER_PRIORITY_MISMATCH);
  }

  @Test
  public void testInConsistentFailoverPriorityVoterCountAcrossStripe_Failure() {
    Cluster configuredCluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency-voter.xml"),
        readContent("failover-priority/tc-config-consistency-voter-1.xml"));

    Cluster newCluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency-voter.xml"),
        readContent("failover-priority/tc-config-consistency-voter-3.xml"));

    testValidationWithException(newCluster, configuredCluster, FAILOVER_PRIORITY_MISMATCH);
  }

  private void testValidationWithException(Cluster newCluster, Cluster configuredCluster, String exceptionString) {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(exceptionString);
    validator.validateAgainst(newCluster, configuredCluster);
  }
}
