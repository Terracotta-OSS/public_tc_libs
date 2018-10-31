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

import com.terracottatech.tools.client.TopologyEntity;
import com.terracottatech.tools.client.TopologyEntityProvider.ConnectionCloseableTopologyEntity;
import com.terracottatech.tools.clustertool.exceptions.ClusterToolException;
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;
import com.terracottatech.tools.command.Command;
import com.terracottatech.tools.command.CommandResult;
import com.terracottatech.tools.command.OutputFormat;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.ConfigurationParser;
import com.terracottatech.tools.config.DefaultConfigurationParser;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.tools.client.TopologyEntityProvider.ENTITY_NAME;
import static com.terracottatech.tools.client.TopologyEntityProvider.ENTITY_VERSION;
import static com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_PARTIAL_FAILURE_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.FAILURE;
import static com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode.PARTIAL_FAILURE;

public class TopologyManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyManager.class);

  private final ConfigurationParser parser = new DefaultConfigurationParser();
  private final CommonEntityManager entityManager = new CommonEntityManager();
  private final StatusManager statusManager = new StatusManager(entityManager);
  private String securityRootDirectory;

  public void setSecurityRootDirectory(String securityRootDirectory) {
    this.securityRootDirectory = securityRootDirectory;
    statusManager.setSecurityRootDirectory(securityRootDirectory);
  }

  public String getSecurityRootDirectory() {
    return securityRootDirectory;
  }

  public void stopCluster(String clusterName, List<String> hostPortList) {
    LOGGER.debug("stopCluster called with: {}, {}", clusterName, hostPortList);
    ConnectionCloseableTopologyEntity entity;
    List<Stripe> stripes;
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();

    try {
      entity = entityManager.getTopologyEntity(hostPortList, securityRootDirectory);
      stripes = entity.getTopologyEntity().getClusterConfiguration().getCluster().getStripes();
      entityManager.releaseTopologyEntity(entity);

      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      Properties connProperties = new Properties();
      connProperties.put(ConnectionPropertyNames.CONNECTION_DISABLE_RECONNECT, "true");

      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          try {
            //We'll stop whatever stripes we can, logging the error/exception we encounter
            ConnectionCloseableTopologyEntity closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory, connProperties);
            entities.add(closeableTopologyEntity);
          } catch (Exception e) {
            LOGGER.debug("Caught exception: {}. Adding into error list", e.getCause());
            errors.add(e);
            LOGGER.error(e.getMessage());
          }
        }));
      }
      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);

      //Validate the configurations of the fetched entities
      for (ConnectionCloseableTopologyEntity closeableTopologyEntity : entities) {
        validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), clusterName);
      }

      //If all validations pass, execute the command
      for (ConnectionCloseableTopologyEntity closeableTopologyEntity : entities) {
        try {
          closeableTopologyEntity.getTopologyEntity().executeCommand(new Command(clusterName, Command.COMMAND_TYPE.STOPS_CLUSTER), true, true);
        } catch (InterruptedException e) {
          //do nothing
        } catch (TimeoutException e) {
          LOGGER.debug("Stop {}", COMMAND_SUCCESS_MESSAGE);
        }
      }
    } finally {
      entityManager.releaseTopologyEntities(entities);
    }

    handleCommandComplete(errors, entities.size());
  }

  public void stopServers(List<String> hostPortList) {
    LOGGER.debug("stopServers called with: {}", hostPortList);
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    hostPortList.parallelStream().forEach(hostPort -> {
      ConnectionCloseableDiagnosticsEntity closeableDiagnosticsEntity = null;
      try {
        closeableDiagnosticsEntity = entityManager.getDiagnosticsEntity(hostPort, securityRootDirectory);
        closeableDiagnosticsEntity.getDiagnostics().forceTerminateServer();
        LOGGER.info(hostPort + ": Stop successful");
      } catch (RuntimeException e) {
        errors.add(e);
        LOGGER.error(hostPort + ": " + e.getMessage());
      } finally {
        entityManager.closeEntity(closeableDiagnosticsEntity);
      }
    });

    handleCommandComplete(errors, hostPortList.size());
  }

  public void dumpClusterState(String clusterName, List<String> hostPortList) {
    LOGGER.debug("dumpClusterState called with: {}, {}", clusterName, hostPortList);
    ConnectionCloseableTopologyEntity entity;
    List<Stripe> stripes;
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();
    AtomicBoolean interrupted = new AtomicBoolean(false);

    try {
      entity = entityManager.getTopologyEntity(hostPortList, securityRootDirectory);
      stripes = entity.getTopologyEntity().getClusterConfiguration().getCluster().getStripes();
      entityManager.releaseTopologyEntity(entity);

      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          try {
            //We'll take dumps on whatever stripes we can, logging the error/exception we encounter
            ConnectionCloseableTopologyEntity closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
            entities.add(closeableTopologyEntity);
          } catch (ClusterToolException e) {
            LOGGER.debug("Caught exception: {}. Adding into error list", e.getCause());
            errors.add(e);
            LOGGER.error(e.getMessage());
          }
        }));
      }
      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);

      //Validate the configurations of the fetched entities
      for (ConnectionCloseableTopologyEntity closeableTopologyEntity : entities) {
        validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), clusterName);
      }

      //If all validations pass, execute the command
      for (ConnectionCloseableTopologyEntity closeableTopologyEntity : entities) {
        try {
          CommandResult result = closeableTopologyEntity.getTopologyEntity().executeCommand(new Command(clusterName, Command.COMMAND_TYPE.DUMP_STATE_CLUSTER));
          LOGGER.debug("Dump executed with result: {}", result.getMessage());
        } catch (InterruptedException e) {
          interrupted.set(true);
        } catch (TimeoutException e) {
          errors.add(e);
          LOGGER.error(e.getMessage());
        }
      }
    } finally {
      entityManager.closeEntities(entities);
      if (interrupted.get()) {
        Thread.currentThread().interrupt();
      }
    }

    handleCommandComplete(errors, entities.size());
  }

  public void dumpServersState(List<String> hostPortList) {
    LOGGER.debug("dumpServersState called with: {}", hostPortList);
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    hostPortList.parallelStream().forEach(hostPort -> {
      ConnectionCloseableDiagnosticsEntity closeableDiagnosticsEntity = null;
      try {
        closeableDiagnosticsEntity = entityManager.getDiagnosticsEntity(hostPort, securityRootDirectory);
        closeableDiagnosticsEntity.getDiagnostics().invoke("L2Dumper", "doServerDump");
        LOGGER.info(hostPort + ": Dump successful");
      } catch (Exception e) {
        errors.add(e);
        LOGGER.error(hostPort + ": " + e.getMessage());
      } finally {
        entityManager.closeEntity(closeableDiagnosticsEntity);
      }
    });

    handleCommandComplete(errors, hostPortList.size());
  }

  public void reloadClusterIPWhitelist(String clusterName, List<String> hostPortList) {
    LOGGER.debug("reloadClusterIPWhitelist called with: {}, {}", clusterName, hostPortList);
    ConnectionCloseableTopologyEntity entity;
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();

    try {
      entity = entityManager.getTopologyEntity(hostPortList, securityRootDirectory);
      entities.add(entity);
      Cluster cluster = validateAndGetClusterConfiguration(entity.getTopologyEntity(), clusterName).getCluster();
      List<Stripe> stripes = cluster.getStripes();

      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          ConnectionCloseableTopologyEntity closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
          entities.add(closeableTopologyEntity);
          validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), clusterName);
        }));
      }

      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);
      reloadIPWhitelist(cluster.hostPortList());

    } finally {
      entityManager.closeEntities(entities);
    }
  }

  public void reloadIPWhitelist(List<String> hostPortList) {
    LOGGER.debug("reloadIPWhitelist called with: {}", hostPortList);
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    hostPortList.parallelStream().forEach(hostPort -> {
      ConnectionCloseableDiagnosticsEntity closeableDiagnosticsEntity = null;
      try {
        closeableDiagnosticsEntity = entityManager.getDiagnosticsEntity(hostPort, securityRootDirectory);
        String invokeResult = closeableDiagnosticsEntity.getDiagnostics().invoke("IPWhitelist", "reload");
        if (invokeResult == null || invokeResult.isEmpty()) {
          LOGGER.info(hostPort + ": IP whitelist reload request successful. Check server logs for details");
        } else {
          LOGGER.debug(invokeResult);
          throw new ClusterToolException(StatusCode.INTERNAL_ERROR, "IP whitelist reload request failed for server at: "
                                                                    + hostPort + ". Check server logs for details.");
        }
      } catch (Exception e) {
        errors.add(e);
        LOGGER.error(hostPort + ": " + e.getMessage());
      } finally {
        entityManager.closeEntity(closeableDiagnosticsEntity);
      }
    });

    handleCommandComplete(errors, hostPortList.size());
  }

  public void showClusterStatus(OutputFormat outputFormat, String clusterName, List<String> hostPortList) {
    LOGGER.debug("showClusterStatus called with: {}, {}, {}", outputFormat, clusterName, hostPortList);
    ConnectionCloseableTopologyEntity entity;
    List<Stripe> stripes;
    ClusterConfiguration configuration;
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();
    int serverCount;

    try {
      entity = entityManager.getTopologyEntity(hostPortList, securityRootDirectory);
      entities.add(entity);
      configuration = validateAndGetClusterConfiguration(entity.getTopologyEntity(), clusterName);
      stripes = configuration.getCluster().getStripes();
      serverCount = configuration.getCluster().getServers().size();

      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          ConnectionCloseableTopologyEntity closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
          entities.add(closeableTopologyEntity);
          validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), clusterName);
        }));
      }

      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);

    } finally {
      entityManager.closeEntities(entities);
    }

    int errorCount = statusManager.doClusterStatus(outputFormat, stripes, configuration);
    throwClusterToolException(errorCount, serverCount, null, null);
  }

  public void showServersStatus(OutputFormat outputFormat, List<String> hostPortList) {
    LOGGER.debug("showServersStatus called with: {}, {}", outputFormat, hostPortList);
    int errorCount = statusManager.doServersStatus(outputFormat, hostPortList);
    throwClusterToolException(errorCount, hostPortList.size() * 2, null, null);
  }

  public Cluster createClusterByServers(List<Server> servers) {
    LOGGER.debug("createClusterByServers called with: {}", servers);
    Set<Stripe> stripes = new CopyOnWriteArraySet<>();
    Map<Server, ClusterToolException> unreachableServersToExceptionsMap = new ConcurrentHashMap<>();

    ExecutorService executorService = Executors.newFixedThreadPool(servers.size());
    List<Future<?>> futures = new ArrayList<>();
    for (Server server : servers) {
      futures.add(executorService.submit(() -> {
        ConnectionCloseableTopologyEntity closeableTopologyEntity = null;
        try {
          closeableTopologyEntity = entityManager.getTopologyEntity(Collections.singletonList(server.getHostPort()), securityRootDirectory);
          List<Stripe> fetchedStripes = validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), null).getCluster().getStripes();
          stripes.addAll(fetchedStripes);
        } catch (ClusterToolException e) {
          if (e.getStatusCode().getCode() == StatusCode.ALREADY_CONFIGURED.getCode()) {
            //If any of the servers is part of a configured cluster, fail immediately
            throw e;
          } else {
            //Else save the server and the exception for later resolution
            unreachableServersToExceptionsMap.put(server, e);
          }
        } finally {
          entityManager.releaseTopologyEntity(closeableTopologyEntity);
        }
      }));
    }

    awaitCompletion(futures);
    shutdownAndAwaitTermination(executorService);

    List<Stripe> stripeList = new ArrayList<>(stripes);
    Iterator<Server> unreachableServersIterator = unreachableServersToExceptionsMap.keySet().iterator();

    for (Stripe stripe : stripeList) {
      while (unreachableServersIterator.hasNext()) {
        Server server = unreachableServersIterator.next();
        if (contains(stripe, server.getHostPort())) {
          //Remove the unreachable servers from stripes whose server(s) we've already contacted
          unreachableServersToExceptionsMap.remove(server);
        }
      }
    }

    if (unreachableServersToExceptionsMap.size() != 0) {
      StringBuilder errorMessage = new StringBuilder();
      boolean firstError = true;
      StatusCode statusCode = null;

      for (ClusterToolException exception : unreachableServersToExceptionsMap.values()) {
        if (firstError) {
          statusCode = exception.getStatusCode();
          firstError = false;
        } else {
          errorMessage.append(", ");
          if (exception.getStatusCode().getCode() != statusCode.getCode()) {
            statusCode = FAILURE;
          }
        }
        errorMessage.append(exception.getMessage());
      }
      throw new ClusterToolException(statusCode, errorMessage.toString());
    }

    LOGGER.debug("Creating a cluster with stripes: {}", stripeList);
    return new Cluster(stripeList);
  }

  /**
   * Method to connect to the provided stripes, fetch their configurations and combine their configurations into a {@link Cluster}.
   * The configurations from the servers are expected to be unique, i.e. the cluster must not have been configured already.
   *
   * Note that this method differs from {@link Cluster#Cluster(List)} in the sense that the latter uses the stripes with
   * their configurations as-is to create the {@code Cluster}, whereas this method uses the input stripes for their servers
   * only, such that it can connect to them and pull their configurations.
   *
   * @param stripes {@link Stripe}s to connect to
   * @return a Cluster created from the fetched Stripe configurations
   * @throws ClusterToolException if any of the input stripes is part of a configured cluster.
   */
  public Cluster createClusterByStripes(List<Stripe> stripes) {
    LOGGER.debug("createClusterByStripes called with: {}", stripes);
    List<List<Stripe>> createdStripes = new ArrayList<>();
    initializeListWithNulls(createdStripes, stripes.size());

    ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
    List<Future<?>> futures = new ArrayList<>();
    for (Stripe stripe : stripes) {
      futures.add(executorService.submit(() -> {
        ConnectionCloseableTopologyEntity closeableTopologyEntity = null;
        try {
          closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
          ClusterConfiguration configuration = validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), null);
          List<Stripe> foundStripes = configuration.getCluster().getStripes();
          LOGGER.debug("Found stripes: " + foundStripes);
          createdStripes.set(stripes.indexOf(stripe), foundStripes);
        } finally {
          entityManager.releaseTopologyEntity(closeableTopologyEntity);
        }
      }));
    }

    awaitCompletion(futures);
    shutdownAndAwaitTermination(executorService);

    List<Stripe> stripeList = new ArrayList<>();
    createdStripes.forEach(stripeList::addAll);
    LOGGER.debug("Creating a cluster with stripes: {}", stripeList);
    return new Cluster(stripeList);
  }

  public void configureByConfigs(String clusterName, List<String> tcConfigs) {
    LOGGER.debug("configureByConfigs called with: {}, {}", clusterName, tcConfigs);
    List<Stripe> stripes = configsToStripes(tcConfigs);
    configureByStripes(clusterName, stripes);
  }

  public void configureByStripes(String clusterName, List<Stripe> stripes) {
    LOGGER.debug("configureByStripes called with: {}, {}", clusterName, stripes);
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();
    try {
      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          ConnectionCloseableTopologyEntity entity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
          // Do not allow cluster reconfiguration
          validateAndGetClusterConfiguration(entity.getTopologyEntity(), null);
          entities.add(entity);
        }));
      }

      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);

      for (int i = 0; i < stripes.size(); i++) {
        try {
          EntityRef<TopologyEntity, ClusterConfiguration, Void> entityRef = entities.get(i).getConnection()
              .getEntityRef(TopologyEntity.class, ENTITY_VERSION, ENTITY_NAME);
          entityRef.reconfigure(new ClusterConfiguration(clusterName, stripes));
          LOGGER.debug("Configure done for servers: {}", stripes.get(i).getServers());
        } catch (EntityNotProvidedException | EntityConfigurationException | EntityNotFoundException e) {
          throw new ClusterToolException(StatusCode.INTERNAL_ERROR, e);
        }
      }
    } finally {
      entityManager.closeEntities(entities);
    }
  }

  public void reConfigure(String clusterName, List<Stripe> stripes) {
    LOGGER.debug("reConfigure called with: {}, {}", clusterName, stripes);
    List<ConnectionCloseableTopologyEntity> entities = new CopyOnWriteArrayList<>();

    try {
      ExecutorService executorService = Executors.newFixedThreadPool(stripes.size());
      List<Future<?>> futures = new ArrayList<>();
      for (Stripe stripe : stripes) {
        futures.add(executorService.submit(() -> {
          ConnectionCloseableTopologyEntity closeableTopologyEntity = entityManager.getTopologyEntity(stripe.serverInetAddresses(), securityRootDirectory);
          validateAndGetClusterConfiguration(closeableTopologyEntity.getTopologyEntity(), clusterName);
          entities.add(closeableTopologyEntity);
        }));
      }

      awaitCompletion(futures);
      shutdownAndAwaitTermination(executorService);

      for (int i = 0; i < entities.size(); i++) {
        EntityRef<TopologyEntity, ClusterConfiguration, Void> entityRef = null;
        ConnectionCloseableTopologyEntity closeableTopologyEntity = entities.get(i);
        ClusterConfiguration prevConfig = closeableTopologyEntity.getTopologyEntity().getClusterConfiguration();
        try {
          entityRef = closeableTopologyEntity.getConnection().getEntityRef(TopologyEntity.class, ENTITY_VERSION, ENTITY_NAME);
          entityRef.reconfigure(new ClusterConfiguration(clusterName, stripes));
          LOGGER.debug("Reconfigure done for servers: {}", stripes.get(i).getServers());
        } catch (EntityNotProvidedException | EntityNotFoundException e) {
          throw new ClusterToolException(StatusCode.INTERNAL_ERROR, e);
        } catch (EntityConfigurationException e) {
          //If an error occurs during reconfigure, try to revert the reconfigured entities to their original state
          LOGGER.debug("Caught {} during reconfigure", e.getCause());
          revertConfiguration(entityRef, prevConfig, i);
          throw new ClusterToolException(StatusCode.CONFLICT, e);
        }
      }
    } finally {
      entityManager.closeEntities(entities);
    }
  }

  private void handleCommandComplete(Collection<Throwable> errors, int totalSize) {
    LOGGER.debug("handleCommandComplete called with {}, {}", errors, totalSize);
    /*
      Eliminates "like" exceptions so we can determine if a bulk action resulted in all like failures or distinct failures.
      Compare returns 0 if the exceptions have the same status code, or if they have the same message (and no code).
     */
    Comparator<Throwable> eliminatingExceptionComparator = (o1, o2) -> {
      if (o1.getClass() == o2.getClass()) {
        //for findbugs, explicit:
        if (o1 instanceof ClusterToolException && o2 instanceof ClusterToolException) {
          return ((ClusterToolException) o1).getStatusCode().getCode() - ((ClusterToolException) o2).getStatusCode().getCode();
        } else {
          return o1.getMessage().compareTo(o2.getMessage());
        }
      }
      return o1.getClass().toString().compareTo(o2.getClass().toString());
    };

    if (errors.size() > 0) {
      TreeSet<Throwable> uniqueSet = new TreeSet<>(eliminatingExceptionComparator);
      uniqueSet.addAll(errors);

      if (uniqueSet.size() > 1) {
        // we have multiple distinct errors.  Throw a "we don't know what happened" error.
        throwClusterToolException(errors.size(), totalSize, "Multiple different failures", null);
      } else {
        throwClusterToolException(errors.size(), totalSize, null, uniqueSet.first());
      }
    } else {
      LOGGER.info(COMMAND_SUCCESS_MESSAGE);
    }
  }

  private void throwClusterToolException(int errorSize, int totalSize, String errorMessage, Throwable throwable) {
    if (errorSize == 0) return;
    StatusCode statusCode = (errorSize < totalSize) ? PARTIAL_FAILURE : FAILURE;
    if (errorMessage == null) {
      errorMessage = (statusCode == PARTIAL_FAILURE) ? COMMAND_PARTIAL_FAILURE_MESSAGE : COMMAND_FAILURE_MESSAGE;
    }
    if (throwable == null) {
      throw new ClusterToolException(statusCode, errorMessage);
    } else {
      throw new ClusterToolException(statusCode, errorMessage, throwable);
    }
  }

  private void revertConfiguration(EntityRef<TopologyEntity, ClusterConfiguration, Void> entityRef,
                                   ClusterConfiguration prevConfig, int failedAt) {
    LOGGER.debug("revertConfiguration called with: {}, {}, {}", entityRef, prevConfig, failedAt);
    for (int j = 0; j < failedAt; j++) {
      try {
        entityRef.reconfigure(prevConfig);
      } catch (EntityNotProvidedException | EntityConfigurationException | EntityNotFoundException ex) {
        // Do nothing
      }
    }
  }

  private List<Stripe> configsToStripes(List<String> tcConfigs) {
    return parser.parseConfigurations(tcConfigs.toArray(new String[0])).getStripes();
  }

  public ClusterConfiguration validateAndGetClusterConfiguration(TopologyEntity entity, String clusterNameToValidate) {
    LOGGER.debug("validateAndGetClusterConfiguration called with: {}, {}", entity, clusterNameToValidate);
    ClusterConfiguration fetchedConfig = entity.getClusterConfiguration();
    String fetchedClusterName = fetchedConfig.getClusterName();

    if (clusterNameToValidate == null) {
      if (fetchedClusterName != null) {
        throw new ClusterToolException(StatusCode.ALREADY_CONFIGURED, "Cluster has been configured" +
            " already with name: " + fetchedClusterName + ". Check your command parameters, or use" +
            " 'reconfigure' command to reconfigure the cluster");
      }
    } else {
      if (fetchedClusterName == null) {
        throw new ClusterToolException(StatusCode.BAD_REQUEST, "Cluster has not been configured." +
            " Configure it using 'configure' command before running other cluster-level commands");
      } else if (!clusterNameToValidate.equals(fetchedClusterName)) {
        throw new ClusterToolException(StatusCode.CONFLICT, "Mismatch found in cluster names. Configured: "
            + fetchedClusterName + ", but provided: " + clusterNameToValidate);
      }
    }

    return fetchedConfig;
  }

  private static boolean contains(Stripe stripe, String hostPort) {
    for (Server server : stripe.getServers()) {
      if (server.getHostPort().equals(hostPort)) {
        return true;
      }
    }
    return false;
  }

  private void awaitCompletion(List<Future<?>> futures) {
    for (Object fetchedItem : getFuturesResult(futures)) {
      if (fetchedItem instanceof Throwable) {
        Throwable throwable = (Throwable) fetchedItem;
        if (throwable instanceof InterruptedException) {
          throw new RuntimeException(throwable);
        }

        if (throwable instanceof ExecutionException) {
          Throwable cause = throwable.getCause();
          if (cause instanceof ClusterToolException) {
            throw new ClusterToolException(((ClusterToolException) cause).getStatusCode(), cause.getMessage());
          } else {
            throw new RuntimeException(cause);
          }
        }
      }
    }
  }

  private List<Object> getFuturesResult(List<Future<?>> futures) {
    List<Object> fetchedItems = new ArrayList<>();
    for (Future<?> future : futures) {
      try {
        fetchedItems.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        fetchedItems.add(e);
      }
    }
    return fetchedItems;
  }

  private void shutdownAndAwaitTermination(ExecutorService executorService) {
    executorService.shutdown();
    try {
      executorService.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeListWithNulls(List<?> list, int size) {
    for (int index = 0; index < size; index++) {
      list.add(null);
    }
  }
}