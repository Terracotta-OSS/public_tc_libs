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
package com.terracottatech.tools.clustertool.managers;


import com.tc.util.Conversion;
import com.terracottatech.License;
import com.terracottatech.LicenseException;
import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import com.terracottatech.licensing.client.LicenseEntityProvider;
import com.terracottatech.licensing.client.LicenseEntityProvider.ConnectionCloseableLicenseEntity;
import com.terracottatech.licensing.client.LicenseInstaller;
import com.terracottatech.tools.client.TopologyEntity;
import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
import com.terracottatech.tools.clustertool.license.DefaultLicenseParser;
import com.terracottatech.tools.clustertool.license.LicenseParser;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.Stripe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;


public class DefaultLicenseManager implements LicenseManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLicenseManager.class);
  private static final LicenseParser LICENSE_PARSER = new DefaultLicenseParser();
  private static final LicenseInstaller LICENSE_INSTALLER = new LicenseInstaller();
  private static final LicenseEntityProvider ENTITY_PROVIDER = new LicenseEntityProvider();
  private final TopologyManager topologyManager;
  private final CommonEntityManager entityManager;

  public DefaultLicenseManager(TopologyManager topologyManager, CommonEntityManager entityManager) {
    this.topologyManager = topologyManager;
    this.entityManager = entityManager;
  }

  @Override
  public boolean installLicense(String clusterName, Cluster cluster, License license) {
    LOGGER.debug("installLicense called with: {}, {}, {}", clusterName, cluster, license);

    if (getLicense(cluster) != null && license.equals(getLicense(cluster))) {
      //If the license that we get here is same as the license already installed, don't install it again
      return false;
    } else {
      ensureClusterIsConfigured(clusterName, cluster);
      ensureCompatibleLicense(cluster, license);
      LICENSE_INSTALLER.installLicense(cluster.serverInetAddresses(), license, getProperties());
      return true;
    }
  }

  @Override
  public void ensureCompatibleLicense(Cluster cluster) {
    LOGGER.debug("ensureCompatibleLicense called with: {}", cluster);
    License license = getLicense(cluster);
    if (license == null) {
      throw new ClusterToolException(StatusCode.BAD_REQUEST, "No license found on the cluster. " +
          "Please run 'configure' command to configure the cluster and install the license.");
    }
    ensureCompatibleLicense(cluster, license);
  }

  @Override
  public void ensureCompatibleLicense(Cluster cluster, License license) {
    LOGGER.debug("ensureCompatibleLicense called with: {}, {}", cluster, license);
    long licenseOffHeapLimitInMB = license.getCapabilityLimitMap().get(DefaultLicenseParser.CAPABILITY_OFFHEAP);
    long totalOffHeapInMB = cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(OFFHEAP))
        .mapToLong(config -> (Long) config.getValue())
        .sum() / Conversion.MemorySizeUnits.MEGA.asBytes();

    if (totalOffHeapInMB > licenseOffHeapLimitInMB) {
      throw new IllegalArgumentException("Cluster offheap resource is not within the limit of the license." +
          " Provided: " + totalOffHeapInMB + " MB, but license allows: " + licenseOffHeapLimitInMB + " MB only");
    }

    long perStripeOffHeapSizeInMB = licenseOffHeapLimitInMB / cluster.getStripes().size();
    Optional<Stripe.Config<?>> configWithHigherOffheap = cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(OFFHEAP))
        .filter(config -> getConfigValueInMB(config) > perStripeOffHeapSizeInMB)
        .findAny();

    if (configWithHigherOffheap.isPresent()) {
      throw new IllegalArgumentException("Stripe offheap resource is not within the per-stripe limit" +
          " of the license. Provided: " + getConfigValueInMB(configWithHigherOffheap.get()) + " MB," +
          " but license allows: " + perStripeOffHeapSizeInMB + " MB only");
    }
  }

  @Override
  public License parse(String licenseFilePath) {
    LOGGER.debug("parse called with: {}", licenseFilePath);
    try {
      return LICENSE_PARSER.parse(licenseFilePath);
    } catch (LicenseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void ensureClusterIsConfigured(String clusterName, Cluster cluster) {
    TopologyEntity topologyEntity = entityManager.getTopologyEntity(cluster.serverInetAddresses(), topologyManager.getSecurityRootDirectory()).getTopologyEntity();
    topologyManager.validateAndGetClusterConfiguration(topologyEntity, clusterName);
  }

  private long getConfigValueInMB(Stripe.Config<?> config) {
    return (Long) config.getValue() / Conversion.MemorySizeUnits.MEGA.asBytes();
  }

  private License getLicense(Cluster cluster) {
    ConnectionCloseableLicenseEntity entity = null;
    try {
      entity = ENTITY_PROVIDER.getEntity(cluster.serverInetAddresses(), getProperties());
      return entity.getLicenseEntity().getLicense();
    } catch (ClusterToolException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      try {
        if (entity != null) {
          entity.close();
        }
      } catch (IOException e) {
        LOGGER.debug("Failed to close entity");
      }
    }
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    String securityRootDirectory = topologyManager.getSecurityRootDirectory();
    if (securityRootDirectory != null) {
      properties.setProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootDirectory);
    }
    return properties;
  }
}
