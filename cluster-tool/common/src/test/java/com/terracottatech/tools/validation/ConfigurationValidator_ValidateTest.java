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

// Tests for ConfigurationValidator::validate
public class ConfigurationValidator_ValidateTest extends BaseConfigurationValidatorTest {
  @Test
  public void testConsistentConfigTypes_Failure() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("config-types/tc-config1.xml"),
        readContent("config-types/tc-config2.xml"));

    testValidationWithException(cluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testConsistentConfigTypes_BackupRestore_Failure() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("config-types/tc-config3.xml"),
        readContent("config-types/tc-config2.xml"));

    testValidationWithException(cluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testConsistentConfigTypes_Success() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("config-types/tc-config3.xml"),
        readContent("config-types/tc-config4.xml"));

    validator.validate(cluster, "");
  }

  @Test
  public void testConsistentOffHeap() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("offheap/tc-config3.xml"),
        readContent("offheap/tc-config4.xml"));

    testValidationWithException(cluster, OFFHEAP_MISMATCH);
  }

  @Test
  public void testConsistentDataRoots_Failure() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("data-roots/tc-config10.xml"),
        readContent("data-roots/tc-config8.xml"));

    testValidationWithException(cluster, DATA_DIRECTORIES_MISMATCH);
  }

  @Test
  public void testConsistentDataRoots_Success() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("data-roots/tc-config7.xml"),
        readContent("data-roots/tc-config8.xml"));

    validator.validate(cluster, "");
  }

  @Test
  public void testInconsistentSecurityAuthenticationConfig() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("security/tc-config-srd-ssl-whitelist-file_auth.xml"),
        readContent("security/tc-config-srd-ssl-whitelist-cert_auth.xml"));

    testValidationWithException(cluster, SECURITY_CONFIGURATION_MISMATCH);
  }

  @Test
  public void testInconsistentSecurityAuditConfig() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("security/tc-config-srd-ssl-whitelist-file_auth.xml"),
        readContent("security/tc-config-srd-ssl-whitelist-file_auth-audit.xml"));

    testValidationWithException(cluster, CONFIG_TYPE_MISMATCH);
  }

  @Test
  public void testConsistentFailoverPriorityAvailability_Success() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-availability.xml"),
        readContent("failover-priority/tc-config-availability-1.xml"));

    validator.validate(cluster, "");
  }

  @Test
  public void testConsistentFailoverPriorityConsistency_Success() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency.xml"),
        readContent("failover-priority/tc-config-consistency-1.xml"));

    validator.validate(cluster, "");
  }

  @Test
  public void testConsistentFailoverPriorityConsistencyVoterCount_Success() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency-voter.xml"),
        readContent("failover-priority/tc-config-consistency-voter-1.xml"));

    validator.validate(cluster, "");
  }

  @Test
  public void testInconsistentFailoverPriority_Failure() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency.xml"),
        readContent("failover-priority/tc-config-availability.xml"));

    testValidationWithException(cluster, FAILOVER_PRIORITY_MISMATCH);
  }

  @Test
  public void testInconsistentFailoverPriorityVoterCount_Failure() {
    Cluster cluster = configurationParser.parseConfigurations(
        readContent("failover-priority/tc-config-consistency-voter-1.xml"),
        readContent("failover-priority/tc-config-consistency-voter-2.xml"));

    testValidationWithException(cluster, FAILOVER_PRIORITY_MISMATCH);
  }

  private void testValidationWithException(Cluster cluster, String exceptionString) {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(exceptionString);
    validator.validate(cluster, "");
  }
}
