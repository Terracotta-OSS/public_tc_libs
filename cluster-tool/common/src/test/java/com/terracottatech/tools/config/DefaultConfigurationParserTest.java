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
package com.terracottatech.tools.config;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.tc.util.Conversion;

import java.util.List;
import java.util.stream.Collectors;

import static com.terracottatech.tools.config.CommandCodecTest.readContent;
import static com.terracottatech.tools.config.Server.DEFAULT_PORT;
import static com.terracottatech.tools.config.Stripe.ConfigType.AUDIT_DIRECTORY;
import static com.terracottatech.tools.config.Stripe.ConfigType.AUTHENTICATION;
import static com.terracottatech.tools.config.Stripe.ConfigType.BACKUP_RESTORE;
import static com.terracottatech.tools.config.Stripe.ConfigType.FAILOVER_PRIORITY;
import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;
import static com.terracottatech.tools.config.Stripe.ConfigType.SECURITY_ROOT_DIRECTORY;
import static com.terracottatech.tools.config.Stripe.ConfigType.SSL_TLS;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST_DEPRECATED;
import static com.terracottatech.tools.config.extractors.FailoverPriorityConfigExtractor.AVAILABILITY_ELEMENT;
import static com.terracottatech.tools.config.extractors.FailoverPriorityConfigExtractor.CONSISTENCY_ELEMENT;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class DefaultConfigurationParserTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConfiguration() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    Cluster cluster = configurationParser.parseConfigurations(readContent("config-types/tc-config1.xml"),
        readContent("config-types/tc-config2.xml"));

    // Should get 2 stripes corresponding to the 2 tc-configs.
    assertEquals(2, cluster.getStripes().size());

    // Asserting configuration of first stripe
    Stripe stripe1 = cluster.getStripes().get(0);
    assertEquals(2, stripe1.getConfigs().size());
    assertEquals(OFFHEAP, stripe1.getConfigs().get(0).getType());
    assertEquals("primary-server-resource", stripe1.getConfigs().get(0).getName());
    assertEquals(Conversion.MemorySizeUnits.GIGA.asBytes() * 768, stripe1.getConfigs().get(0).getValue());

    assertEquals(2, stripe1.getServers().size());
    assertEquals("tc-bigmemory-03.eur.ad.sag", stripe1.getServers().get(0).getHost());
    assertEquals(9510, stripe1.getServers().get(0).getPort());
    assertEquals("tc-bigmemory-04.eur.ad.sag", stripe1.getServers().get(1).getHost());
    assertEquals(9510, stripe1.getServers().get(1).getPort());

    // Asserting configuration of second stripe
    Stripe stripe2 = cluster.getStripes().get(1);
    assertEquals(5, stripe2.getConfigs().size());

    assertEquals(Stripe.ConfigType.DATA_DIRECTORIES, stripe2.getConfigs().get(1).getType());
    assertEquals("root", stripe2.getConfigs().get(1).getName());
    assertEquals("../data", stripe2.getConfigs().get(1).getValue());

    assertEquals(OFFHEAP, stripe2.getConfigs().get(0).getType());
    assertEquals("primary-server-resource", stripe2.getConfigs().get(0).getName());
    assertEquals(Conversion.MemorySizeUnits.MEGA.asBytes() * 64, stripe2.getConfigs().get(0).getValue());

    assertEquals(Stripe.ConfigType.PLATFORM_PERSISTENCE, stripe2.getConfigs().get(3).getType());
    assertEquals("root", stripe2.getConfigs().get(3).getName());
    assertEquals("", stripe2.getConfigs().get(3).getValue());

    assertEquals(1, stripe2.getServers().size());
    assertEquals("testServer0", stripe2.getServers().get(0).getName());
    assertEquals("localhost", stripe2.getServers().get(0).getHost());
    assertEquals(4164, stripe2.getServers().get(0).getPort());
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithSslAndWhitelist() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(4));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, WHITELIST, SECURITY_ROOT_DIRECTORY, SSL_TLS));
  }

  @Test
  public void testValidSecurityConfiguration_NoSecurityRootDirWithDeprecatedWhitelist() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-no-srd-whitelist_deprecated.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(2));
    assertThat(configs.get(0).getType(), is(WHITELIST_DEPRECATED));
  }

  @Test
  public void testValidSecurityConfiguration_NoSecurityRootDirWithDeprecatedWhitelistAndAudit() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-no-srd-audit-whitelist_deprecated.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(3));
    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(AUDIT_DIRECTORY, WHITELIST_DEPRECATED, FAILOVER_PRIORITY));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithSslAndFileAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ssl-file_auth.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(4));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY, SSL_TLS));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithSslAndLdapAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ssl-ldap_auth.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(4));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY, SSL_TLS));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithLdapAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ldap_auth.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(3));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithFileAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-file_auth.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(3));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithCertificateAuthenticationAndWhitelist() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist-cert_auth.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(5));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY, SSL_TLS, WHITELIST));
  }

  @Test
  public void testValidSecurityConfiguration_SecurityRootDirWithSslAndFileAuthenticationAndAudit() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist-file_auth-audit.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.size(), is(6));

    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()),
        containsInAnyOrder(FAILOVER_PRIORITY, AUTHENTICATION, SECURITY_ROOT_DIRECTORY, SSL_TLS, WHITELIST, AUDIT_DIRECTORY));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSecurityConfiguration_SecurityRootDirOnly() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-srd-only.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSecurityConfiguration_SecurityRootDirAudit() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-srd-audit.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSecurityConfiguration_SecurityRootDirWithDeprecatedWhitelist() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-srd-ssl-whitelist_deprecated.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidSecurityConfiguration_SecurityRootDirWithCertificateAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-srd-cert_auth.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSecurityConfiguration_NoSecurityRootDirSslOnly() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-no-srd-ssl.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSecurityConfiguration_NoSecurityRootDirWhitelistOnly() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-no-srd-whitelist.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidSecurityConfiguration_NoSecurityRootDirWithAudit() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-no-srd-audit.xml"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidSecurityConfiguration_NoSecurityRootDirWithAuthentication() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    parser.parseConfigurations(readContent("security/tc-config-no-srd-auth.xml"));
  }

  @Test
  public void testBackupRestoreConfiguration() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("config-types/tc-config4.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.stream().map(Stripe.Config::getType).collect(Collectors.toSet()), hasItem(BACKUP_RESTORE));
  }

  @Test
  public void testDataRootAttributeSingleRoot() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("data-roots/tc-config7.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.get(1).getName(), is("use-for-platform"));
  }

  @Test
  public void testDataRootAttributeSingleRootExplicit() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("data-roots/tc-config8.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.get(1).getName(), is("use-for-platform"));
  }

  @Test
  public void testDataRootAttributeMultipleRoot() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("data-roots/tc-config10.xml"));
    List<Stripe.Config<?>> configs = cluster.getStripes().get(0).getConfigs();
    assertThat(configs.get(2).getName(), is("use-for-platform"));
  }

  @Test
  public void testMinimalTCConfig() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("No <server> elements found in tc-config");
    configurationParser.parseConfigurations(readContent("minimal/tc-config-minimal.xml"));
  }

  @Test
  public void testMinimalTCConfig_noHostAttribute() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid 'host' attribute");
    configurationParser.parseConfigurations(readContent("minimal/tc-config-minimal-no-host.xml"));
  }

  @Test
  public void testInvalidTsaPort() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid tsa-port");
    configurationParser.parseConfigurations(readContent("invalid/tc-config-invalid-port.xml"));
  }

  @Test
  public void testMinimalTCConfig_noNameAttribute() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    Cluster cluster = configurationParser.parseConfigurations(readContent("minimal/tc-config-minimal-no-name.xml"));
    Stripe stripe1 = cluster.getStripes().get(0);

    assertEquals("MCSAAG02", stripe1.getServers().get(0).getHost());
    assertEquals("MCSAAG02" + ":" + DEFAULT_PORT, stripe1.getServers().get(0).getName());
    assertEquals(DEFAULT_PORT, stripe1.getServers().get(0).getPort());
  }

  @Test
  public void testParameterSubstitutionInHost() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid 'host' attribute");
    configurationParser.parseConfigurations(readContent("minimal/tc-config-substitutable-host.xml"));
  }

  @Test
  public void testParameterSubstitutionInName() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    Cluster cluster = configurationParser.parseConfigurations(readContent("minimal/tc-config-substitutable-name.xml"));
    Stripe stripe1 = cluster.getStripes().get(0);
    assertEquals("localhost", stripe1.getServers().get(0).getHost());
    assertEquals("%c", stripe1.getServers().get(0).getName());//We'd allow this in the parser
  }

  @Test
  public void testIPv6AddressesParsing() {
    ConfigurationParser configurationParser = new DefaultConfigurationParser();
    Cluster cluster = configurationParser.parseConfigurations(readContent("startup/tc-config-ipv6.xml"));

    Stripe stripe = cluster.getStripes().get(0);
    assertEquals("::1", stripe.getServers().get(0).getName());
    assertEquals("[2010:aaaa::2]", stripe.getServers().get(0).getHost());
    assertEquals("[2010:aaaa::2]:" + DEFAULT_PORT, stripe.getServers().get(0).getHostPort());
  }

  @Test
  public void testFailoverPriorityAvailabilityConfiguration() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("failover-priority/tc-config-availability.xml"));
    assertThat(cluster.getStripes().get(0).getConfigs().size(), is(1));
    Stripe.Config<?> config = cluster.getStripes().get(0).getConfigs().get(0);
    assertThat(config.getType(), is(FAILOVER_PRIORITY));
    assertThat(config.getName(), is(AVAILABILITY_ELEMENT));
    assertThat(config.getValue(), is("-1"));
  }

  @Test
  public void testFailoverPriorityConsistencyConfiguration() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("failover-priority/tc-config-consistency.xml"));
    assertThat(cluster.getStripes().get(0).getConfigs().size(), is(1));
    Stripe.Config<?> config = cluster.getStripes().get(0).getConfigs().get(0);
    assertThat(config.getType(), is(FAILOVER_PRIORITY));
    assertThat(config.getName(), is(CONSISTENCY_ELEMENT));
    assertThat(config.getValue(), is("0"));
  }

  @Test
  public void testFailoverPriorityConsistencyVoterConfiguration() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("failover-priority/tc-config-consistency-voter.xml"));
    assertThat(cluster.getStripes().get(0).getConfigs().size(), is(1));
    Stripe.Config<?> config = cluster.getStripes().get(0).getConfigs().get(0);
    assertThat(config.getType(), is(FAILOVER_PRIORITY));
    assertThat(config.getName(), is(CONSISTENCY_ELEMENT));
    assertThat(config.getValue(), is("1"));
  }

  @Test
  public void testDontValidateProperty() {
    DefaultConfigurationParser parser = new DefaultConfigurationParser();
    Cluster cluster = parser.parseConfigurations(readContent("no-validate/tc-config.xml"));
    assertThat(cluster.getStripes().get(0).getConfigs().size(), is(1));
    assertFalse(cluster.getStripes().get(0).shouldValidateStrictly());
  }
}
