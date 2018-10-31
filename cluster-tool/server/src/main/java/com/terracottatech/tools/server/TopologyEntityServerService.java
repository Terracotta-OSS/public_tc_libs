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
package com.terracottatech.tools.server;

import com.tc.classloader.PermanentEntity;
import com.terracottatech.security.audit.AuditLogger;
import com.terracottatech.security.audit.NoopAuditLogger;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.server.detailed.state.DetailedServerState;
import com.terracottatech.tools.validation.ConfigurationValidator;
import com.terracottatech.tools.validation.Validator;
import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.command.runnelcodecs.ClusterConfigurationRunnelCodec;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.command.CommandCodec;
import com.terracottatech.tools.config.CodecUtil;
import com.terracottatech.tools.config.ConfigurationParser;
import com.terracottatech.tools.config.DefaultConfigurationParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.CommonServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.NoConcurrencyStrategy;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.monitoring.PlatformService;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import static com.tc.objectserver.core.api.GuardianContext.getCurrentChannelProperties;
import static com.terracottatech.security.audit.AuditUtil.getClientIpAddress;
import static com.terracottatech.security.audit.AuditUtil.getClientId;
import static com.terracottatech.security.audit.AuditUtil.getUsername;

@PermanentEntity(type = "com.terracottatech.tools.client.TopologyEntity", names = { "topology-entity" })
public class TopologyEntityServerService implements EntityServerService<Command, CommandResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyEntityServerService.class);
  private static final Validator VALIDATOR = new ConfigurationValidator();
  private static final long ENTITY_VERSION = 1L;
  private static final String ENTITY_TYPE = "com.terracottatech.tools.client.TopologyEntity";

  private volatile boolean strictStartupValidation = true;

  @Override
  public long getVersion() {
    return ENTITY_VERSION;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return ENTITY_TYPE.equals(typeName);
  }

  @Override
  public TopologyActiveEntity createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    DetailedServerState detailedServerState = new DetailedServerState();
    detailedServerState.init();

    ClusterConfiguration clusterConfiguration = getEntityConfiguration(configuration);
    PlatformService platformService = getPlatformService(registry);
    ClusterConfiguration startupConfiguration = getStartupConfiguration(platformService);

    if (clusterConfiguration == null) {
      clusterConfiguration = startupConfiguration;
    } else {
      // Since entity configuration is non-null, cluster must have been configured - validate configurations now
      if (strictStartupValidation) {
        validateStartupConfiguration(startupConfiguration, clusterConfiguration);
      } else {
        // Failover after a reconfigure without a restart: startup configuration may not have changed
        validateConfiguration(clusterConfiguration, startupConfiguration);
      }
    }

    LOGGER.debug("Creating TopologyActiveEntity with: {}", clusterConfiguration);
    return new TopologyActiveEntity(clusterConfiguration, getAuditLogger(registry), platformService);
  }

  @Override
  public TopologyPassiveEntity createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    DetailedServerState detailedServerState = new DetailedServerState();
    detailedServerState.init();

    ClusterConfiguration clusterConfiguration = getEntityConfiguration(configuration);
    PlatformService platformService = getPlatformService(registry);
    ClusterConfiguration startupConfiguration = getStartupConfiguration(platformService);

    if (clusterConfiguration == null) {
      clusterConfiguration = startupConfiguration;
    } else {
      // Since entity configuration is non-null, cluster must have been configured - validate configurations now
      validateStartupConfiguration(startupConfiguration, clusterConfiguration);
    }

    LOGGER.debug("Creating TopologyPassiveEntity with: {}", clusterConfiguration);
    return new TopologyPassiveEntity(clusterConfiguration, getAuditLogger(registry), platformService);
  }

  @Override
  public ConcurrencyStrategy<Command> getConcurrencyStrategy(byte[] configuration) {
    return new NoConcurrencyStrategy<>();
  }

  @Override
  public CommandCodec getMessageCodec() {
    return CommandCodec.MESSAGE_CODEC;
  }

  @Override
  public SyncMessageCodec<Command> getSyncMessageCodec() {
    return null;
  }

  @Override
  public <AP extends CommonServerEntity<Command, CommandResult>> AP reconfigureEntity(ServiceRegistry registry, AP oldEntity, byte[] configuration) throws ConfigurationException {
    PlatformService platformService = getPlatformService(registry);
    ClusterConfiguration newClusterConfiguration = ClusterConfigurationRunnelCodec.decode(configuration);
    LOGGER.info("Reconfiguring TopologyEntity with: {}", newClusterConfiguration);

    ClusterConfiguration prevClusterConfiguration;
    AuditLogger auditLogger = getAuditLogger(registry);

    if (oldEntity instanceof ActiveServerEntity) {
      prevClusterConfiguration = ((TopologyActiveEntity) oldEntity).getClusterConfiguration();
      validateAndAudit(newClusterConfiguration, prevClusterConfiguration, auditLogger, true);
      return (AP) new TopologyActiveEntity(newClusterConfiguration, auditLogger, platformService);
    } else if (oldEntity instanceof PassiveServerEntity){
      prevClusterConfiguration = ((TopologyPassiveEntity) oldEntity).getClusterConfiguration();
      validateAndAudit(newClusterConfiguration, prevClusterConfiguration, auditLogger, false);
      return (AP) new TopologyPassiveEntity(newClusterConfiguration, auditLogger, platformService);
    } else {
      throw new AssertionError("Unknown entity type: " + oldEntity);
    }
  }

  private void validateAndAudit(ClusterConfiguration newConfig, ClusterConfiguration prevConfig, AuditLogger auditLogger,
                                boolean isActive) throws ConfigurationException {
    if (prevConfig.getClusterName() == null) {
      if (isActive) {
        auditOnActive(auditLogger, "Configure", newConfig);
        validateConfiguration(newConfig, prevConfig);
      } else {
        auditOnPassive(auditLogger, "Configure", newConfig);
        validateStartupConfiguration(prevConfig, newConfig);
      }
    } else {
      if (isActive) {
        auditOnActive(auditLogger, "Reconfigure", newConfig);
        validateServers(newConfig, prevConfig);
        validateConfiguration(newConfig, prevConfig);
      } else {
        auditOnPassive(auditLogger, "Reconfigure", newConfig);
        validateServers(newConfig, prevConfig);
        validateConfiguration(newConfig, prevConfig);
        strictStartupValidation = false;
      }
    }
  }

  @Override
  public ExecutionStrategy<Command> getExecutionStrategy(byte[] configuration) {
    return new CommandExecutionStrategy();
  }

  ClusterConfiguration getEntityConfiguration(byte[] configuration) {
    ClusterConfiguration clusterConfiguration = null;
    if (configuration != null && configuration.length > 0) {
      clusterConfiguration = ClusterConfigurationRunnelCodec.decode(configuration);
    }
    return clusterConfiguration;
  }

  private PlatformService getPlatformService(ServiceRegistry registry) {
    PlatformService platformService;
    try {
      platformService = registry.getService(() -> PlatformService.class);
    } catch (ServiceException e) {
      throw new AssertionError("Failed to obtain the singleton PlatformService instance", e);
    }
    if (platformService == null) {
      throw new AssertionError("Server failed to load PlatformService.");
    }
    return platformService;
  }

  ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
    InputStream stream = platformService.getPlatformConfiguration();
    if (stream == null) {
      throw new NullPointerException("Stripe Config returned by platform service cannot be null.");
    }

    try {
      ConfigurationParser configurationParser = new DefaultConfigurationParser();
      //Inspection of the source of this data indicates it is UTF-8. Why it must be passed to us as bytes I don't know.
      Cluster cluster = configurationParser.parseConfigurations(new String(CodecUtil.streamToBytes(stream), "UTF-8"));
      ClusterConfiguration clusterConfiguration = new ClusterConfiguration(cluster);
      LOGGER.debug("Returning {} from getStartupConfiguration", clusterConfiguration);
      return clusterConfiguration;
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }

  private void validateStartupConfiguration(ClusterConfiguration startupConfiguration, ClusterConfiguration installedConfiguration) throws ConfigurationException {
    LOGGER.debug("validateStartupConfiguration called with: {}, {}", startupConfiguration, installedConfiguration);
    try {
      getValidator().validateStripeAgainstCluster(startupConfiguration.getCluster().getStripes().get(0),
                                                  installedConfiguration.getCluster());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
  }

  private void validateConfiguration(ClusterConfiguration newClusterConfiguration, ClusterConfiguration prevClusterConfiguration) throws ConfigurationException {
    LOGGER.debug("validateConfiguration called with: {}, {}", newClusterConfiguration, prevClusterConfiguration);
    try {
      getValidator().validateAgainst(newClusterConfiguration.getCluster(), prevClusterConfiguration.getCluster());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(e.getMessage(), e);
    }
  }

  Validator getValidator() {
    return VALIDATOR;
  }

  private void validateServers(ClusterConfiguration newClusterConfiguration, ClusterConfiguration prevClusterConfiguration) throws ConfigurationException {
    LOGGER.debug("validateServers called with: {}, {}", newClusterConfiguration, prevClusterConfiguration);
    List<Server> newServers = newClusterConfiguration.getCluster().getServers();
    List<Server> previousServers = prevClusterConfiguration.getCluster().getServers();
    if (!compareServers(newServers, previousServers)) {
      throw new ConfigurationException("Mismatched stripes. Provided: " + newServers + ", but previously known: " + previousServers +
          "\nThe order and number of stripes, and the <server> elements must match the recorded cluster configuration.");
    }
  }

  private boolean compareServers(List<Server> newServers, List<Server> previousServers) {
    LOGGER.debug("compareServers called with: {}, {}", newServers, previousServers);
    if (newServers.size() != previousServers.size()) {
      return false;
    }

    for (int i = 0; i < newServers.size(); i++) {
      Server newServer = newServers.get(i);
      Server previousServer = previousServers.get(i);
      // If server name contains a substitutable parameter, only compare host and port
      if ((newServer.getName().contains("%") || previousServer.getName().contains("%"))) {
        if (!newServer.getHostPort().equals(previousServer.getHostPort())) {
          return false;
        }
      } else {
        if (!newServer.equals(previousServer)) {
          return false;
        }
      }
    }
    return true;
  }

  private void auditOnActive(AuditLogger auditLogger, String command, ClusterConfiguration newConfig) {
    Properties properties = getCurrentChannelProperties();
    String username = getUsername(properties);

    if (username == null) {
      auditLogger.info(command + " invoked:- IP: {}, ClientId: {}, Cluster Name: {}, Configuration: {}",
          getClientIpAddress(properties), getClientId(properties), newConfig.getClusterName(), newConfig.getCluster());
    } else {
      auditLogger.info(command + " invoked:- IP: {}, User: {}, ClientId: {}, Cluster Name: {}, Configuration: {}",
          getClientIpAddress(properties), getUsername(properties), getClientId(properties), newConfig.getClusterName(), newConfig.getCluster());
    }
  }

  private void auditOnPassive(AuditLogger auditLogger, String command, ClusterConfiguration newConfig) {
    auditLogger.info(command + " invoked:- ClientId: {}, Cluster Name: {}, Configuration: {}",
        getClientId(getCurrentChannelProperties()), newConfig.getClusterName(), newConfig.getCluster());
  }

  private AuditLogger getAuditLogger(ServiceRegistry registry) {
    try {
      AuditLogger auditLogger = registry.getService(() -> AuditLogger.class);
      if (auditLogger == null) {
        auditLogger = new NoopAuditLogger();
      }
      return auditLogger;
    } catch (ServiceException e) {
      throw new AssertionError("Multiple AuditLogger implementations found", e);
    }
  }
}
