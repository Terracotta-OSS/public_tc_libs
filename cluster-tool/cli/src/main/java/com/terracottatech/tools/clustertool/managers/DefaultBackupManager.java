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

import com.terracottatech.br.common.BackupExecutionRequest;
import com.terracottatech.br.common.BackupExecutionResult;
import com.terracottatech.tools.client.TopologyEntity;
import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.br.common.BackupExecutionRequest.*;
import static com.terracottatech.br.common.BackupExecutionResult.IN_PROGRESS;
import static com.terracottatech.br.common.BackupExecutionResult.SUCCESS;
import static com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity;

public class DefaultBackupManager implements BackupManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackupManager.class);

  public static final String DIAGNOSTIC_NAME = "Backup";

  private static final long TIME_BETWEEN_MONITOR_BACKUP_IN_MILLISEC = 2000;

  private final TopologyManager topologyManager;
  private final CommonEntityManager entityManager;

  private final Map<String, ConnectionCloseableDiagnosticsEntity> hostPortDiagnosticsMap;
  private final Map<String, String> hostPortStateMap;
  private volatile ExecutorService threadPool;

  public DefaultBackupManager(TopologyManager topologyManager, CommonEntityManager entityManager) {
    this.topologyManager = topologyManager;
    this.entityManager = entityManager;
    this.hostPortDiagnosticsMap = new ConcurrentHashMap<>();
    this.hostPortStateMap = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void backup(String clusterName, List<String> hostPortList) {
    TopologyEntity topologyEntity = entityManager.getTopologyEntity(hostPortList, topologyManager.getSecurityRootDirectory()).getTopologyEntity();
    ClusterConfiguration clusterConfiguration = topologyManager.validateAndGetClusterConfiguration(topologyEntity, clusterName);

    // Sizing the thread pool according to number of servers in cluster, as we max fan-out to all the servers.
    this.threadPool = Executors.newFixedThreadPool(clusterConfiguration.getCluster().hostPortList().size());
    populateHostPortDiagnosticsMap(clusterConfiguration.getCluster());

    try {
      String backupName = UUID.randomUUID().toString();
      LOGGER.info("\n\nPHASE 0: SETTING BACKUP NAME TO : {}", backupName);
      prePrepareBackup(clusterConfiguration.getCluster(), backupName);
      if (clusterConfiguration.getCluster().getStripes().size() == 1) {
        singleStripeBackup(clusterConfiguration.getCluster());
      } else {
        multiStripeBackup(clusterConfiguration.getCluster());
      }
    } finally {
      this.threadPool.shutdown();
      this.threadPool = null;
      closeEntitiesAndClearMap(hostPortDiagnosticsMap);
      this.hostPortStateMap.clear();
    }

    LOGGER.info(ClusterToolCommandResults.COMMAND_SUCCESS_MESSAGE);
  }

  private void closeEntitiesAndClearMap(Map<String, ConnectionCloseableDiagnosticsEntity> hostPortDiagnosticsMap) {
    hostPortDiagnosticsMap.values().forEach(connectionCloseableDiagnosticsEntity -> {
      try {
        connectionCloseableDiagnosticsEntity.close();
      } catch (IOException e) {
        LOGGER.debug("Failed to close diagnostic entity with exception: {}", e);
      }
    });
    hostPortDiagnosticsMap.clear();
  }

  private void populateHostPortDiagnosticsMap(Cluster cluster) {
    // ExecutorService to create the diagnostics entities as don't want to block the main thread pool.
    ExecutorService executorService = Executors.newFixedThreadPool(cluster.hostPortList().size());

    // Waiting to get active diagnostics from all the stripes.
    CountDownLatch latch = new CountDownLatch(1);

    // Keeping track of running threads
    AtomicInteger threadCount = new AtomicInteger(cluster.hostPortList().size());

    // Keeping track of exception in executor service thread.
    AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();

    // For each of the hostPort, submit it to the executor service to get the diagnostics.
    cluster.hostPortList()
        .parallelStream()
        .forEach(hostPort -> executorService.execute(() -> {
          try {
            hostPortDiagnosticsMap.put(hostPort, entityManager.getDiagnosticsEntity(hostPort, topologyManager.getSecurityRootDirectory()));

            // Once all active diagnostics are available then count down the latch.
            if (isAllActivesDiagnosticsAvailable(cluster)) {
              latch.countDown();
            }
          } catch (RuntimeException e) {

            // If timeout occurs then handle it gracefully
            if (contains(e, TimeoutException.class)) {
              return;
            }

            // Otherwise, propagate the exception.
            exceptionAtomicReference.set(e);
            latch.countDown();
          } finally {
            threadCount.decrementAndGet();

            // Count Down the latch if no other thread is running.
            if (threadCount.get() == 0) {
              latch.countDown();
            }
          }
        }));

    // Wait for the Count Down latch to come down.
    try {
      latch.await();
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }

    // Shutdown the created executor service.
    executorService.shutdownNow();

    // Propagate the exception occurred on executor service thread.
    if (exceptionAtomicReference.get() != null) {
      throw new IllegalStateException(exceptionAtomicReference.get());
    }
  }

  private boolean isAllActivesDiagnosticsAvailable(Cluster cluster) {
    long activeCount = hostPortDiagnosticsMap.entrySet()
        .parallelStream()
        .filter(entry -> {
          String state = hostPortStateMap.computeIfAbsent(entry.getKey(), hostPort -> entry.getValue().getDiagnostics().getState());
          return state.contains("ACTIVE");
        })
        .count();

    return activeCount == cluster.getStripes().size();
  }

  private <T> boolean contains(Throwable e, Class<T> cls) {
    while (true) {
      if (e == null) {
        return false;
      }
      if (e.getClass() == cls) {
        return true;
      }
      e = e.getCause();
    }
  }

  private void prePrepareBackup(Cluster cluster, String backupName) {
    LOGGER.debug("Setting common backup name {} on all servers of cluster {}.", backupName, cluster.serverInetAddresses());

    List<Server> failedServers = sendBackupNameToAllServers(cluster.getServers(), backupName, createServerStripeNameMapping(cluster))
        .filter(backupResult -> backupResult.getBackupExecutionResult() == BackupExecutionResult.BACKUP_FAILURE)
        .map(BackupResult::getServer)
        .collect(Collectors.toList());

    if (!failedServers.isEmpty()) {
      throw new IllegalStateException(
          String.format("Unable to start backup as one or more servers failed to accept the backup request. " +
                        "Either another backup is in progress or there is a configuration error." +
                        "%nServers that failed are '%s'" +
                        "%nPlease check server logs for more details", failedServers));
    }
  }

  private void singleStripeBackup(Cluster cluster) {
    try {
      LOGGER.info("\n\nPHASE (1/3): {}", PREPARE_AND_ENTER_ONLINE_BACKUP_MODE.name());
      Server server = prepareAndEnterBackup(cluster.getStripes().get(0));

      LOGGER.info("\n\nPHASE (2/3): {}", START_BACKUP.name());
      startAndMonitorBackup(Arrays.asList(server));

      LOGGER.info("\n\nPHASE (3/3): {}", EXIT_ONLINE_BACKUP_MODE.name());
      exitOnlineBackupMode(Arrays.asList(server));

    } catch (Exception e) {
      LOGGER.info("\n\nPHASE (CLEANUP): {}", ABORT_BACKUP.name());
      abortBackup(cluster.getStripes());
      throw e;
    }
  }

  private void multiStripeBackup(Cluster cluster) {
    try {
      LOGGER.info("\n\nPHASE (1/4): {}", PREPARE_FOR_BACKUP.name());
      List<Server> servers = prepareBackup(cluster.getStripes());

      LOGGER.info("\n\nPHASE (2/4): {}", ENTER_ONLINE_BACKUP_MODE.name());
      enterBackup(servers);

      LOGGER.info("\n\nPHASE (3/4): {}", START_BACKUP.name());
      startAndMonitorBackup(servers);

      LOGGER.info("\n\nPHASE (4/4): {}", EXIT_ONLINE_BACKUP_MODE.name());
      exitOnlineBackupMode(servers);

    } catch (Exception e) {
      LOGGER.info("\n\nPHASE (CLEANUP): {}", ABORT_BACKUP.name());
      abortBackup(cluster.getStripes());
      throw e;
    }
  }

  private List<Server> startBackup(List<Server> servers) {
    Map<Server, List<BackupResult>> serverBackupResults = sendRequestToAllServers(servers, START_BACKUP)
        .filter(backupResult -> Arrays.asList(IN_PROGRESS, SUCCESS).contains(backupResult.getBackupExecutionResult()))
        .collect(Collectors.groupingBy(BackupResult::getServer));

    if (!serverBackupResults.keySet().containsAll(servers)) {
      List<Server> failedServers = new ArrayList<>(servers);
      failedServers.removeAll(serverBackupResults.keySet());
      throw new IllegalStateException(String.format("Backup failed as few servers '%s', failed to start backup.", failedServers));
    }


    // Identify the servers where backup succeeded.
    List<Server> succeededServers = serverBackupResults.entrySet().stream()
        .filter(entry -> entry.getValue().stream().anyMatch(backupResult -> backupResult.backupExecutionResult == SUCCESS))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
    if (!succeededServers.isEmpty()) {
      LOGGER.debug("Backup Succeeded on servers: {}", succeededServers);
    }

    // Return the servers where the backup is in progress
    return serverBackupResults.entrySet().stream()
        .filter(entry -> entry.getValue().stream().anyMatch(backupResult -> backupResult.backupExecutionResult == IN_PROGRESS))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
  }

  private void startAndMonitorBackup(List<Server> servers) {
    LOGGER.debug("Starting backup on: {}.", servers);
    servers = startBackup(servers);

    while (true) {
      if (servers.isEmpty()) return;
      sleep(TIME_BETWEEN_MONITOR_BACKUP_IN_MILLISEC);
      LOGGER.info("..............................");
      LOGGER.debug("Monitoring backup on: {}.", servers);
      servers = startBackup(servers);
    }
  }

  private void sleep(long timeInMillisec) {
    try {
      LOGGER.debug("Sleeping for {} milliseconds.", timeInMillisec);
      Thread.sleep(timeInMillisec);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private List<Server> prepareBackup(List<Stripe> stripes) {
    LOGGER.debug("Preparing backup on servers of {}.", stripes);
    Map<Stripe, List<BackupResult>> serverBackupResults = sendRequestToAllStripes(stripes, BackupExecutionRequest.PREPARE_FOR_BACKUP)
        .filter(backupResult -> backupResult.getBackupExecutionResult() == BackupExecutionResult.SUCCESS)
        .collect(Collectors.groupingBy(BackupResult::getStripe));

    if (!serverBackupResults.keySet().containsAll(stripes)) {
      List<Stripe> failedStripes = new ArrayList<>(stripes);
      failedStripes.removeAll(serverBackupResults.keySet());
      throw new IllegalStateException(String.format("Backup failed as some stripes '%s', failed to prepare for backup.", failedStripes));
    }

    return serverBackupResults.values().stream()
        .flatMap(backupResults -> backupResults.stream())
        .map(BackupResult::getServer)
        .collect(Collectors.toList());
  }

  private void enterBackup(List<Server> servers) {
    LOGGER.debug("Entering backup mode on servers {}.", servers);
    Map<Server, List<BackupResult>> serverBackupResults = sendRequestToAllServers(servers, BackupExecutionRequest.ENTER_ONLINE_BACKUP_MODE)
        .filter(backupResult -> backupResult.getBackupExecutionResult() == BackupExecutionResult.SUCCESS)
        .collect(Collectors.groupingBy(BackupResult::getServer));

    if (!serverBackupResults.keySet().containsAll(servers)) {
      List<Server> failedServers = new ArrayList<>(servers);
      failedServers.removeAll(serverBackupResults.keySet());
      throw new IllegalStateException(String.format("Backup failed as some servers '%s', failed to enter online backup mode.", failedServers));
    }
  }

  private Server prepareAndEnterBackup(Stripe stripe) {
    LOGGER.debug("Entering backup mode on servers of {}.", stripe);
    Optional<BackupResult> backupResultOptional = sendRequestToAllServers(stripe.getServers(), PREPARE_AND_ENTER_ONLINE_BACKUP_MODE)
        .filter(backupResult -> backupResult.getBackupExecutionResult() == SUCCESS)
        .findAny();

    if (!backupResultOptional.isPresent()) {
      throw new IllegalStateException(String.format("Backup failed as no server enters into backup mode within %s", stripe));
    }

    return backupResultOptional.get().getServer();
  }

  private void abortBackup(List<Stripe> stripes) {
    LOGGER.debug("Aborting backup on servers of stripes: {}.", stripes);
    sendRequestToAllStripes(stripes, ABORT_BACKUP)
        .filter(backupResult -> false) // Discard all responses
        .findAny();
  }

  private void exitOnlineBackupMode(List<Server> servers) {
    LOGGER.debug("Exiting backup mode from servers {}.", servers);
    Map<Server, List<BackupResult>> serverBackupResults = sendRequestToAllServers(servers, EXIT_ONLINE_BACKUP_MODE)
        .filter(backupResult -> backupResult.backupExecutionResult == SUCCESS)
        .collect(Collectors.groupingBy(BackupResult::getServer));

    if (!serverBackupResults.keySet().containsAll(servers)) {
      List<Server> failedServers = new ArrayList<>(servers);
      failedServers.removeAll(serverBackupResults.keySet());
      throw new IllegalStateException(String.format("Backup failed as few servers '%s', failed to exit online backup.", failedServers));
    }
  }

  private Stream<BackupResult> sendRequestToAllStripes(List<Stripe> stripes, BackupExecutionRequest backupExecutionRequest) {
    return stripes.parallelStream()
        .flatMap(stripe -> sendRequestToAllServers(stripe.getServers(), backupExecutionRequest)
            .map(backupResult -> {
              backupResult.setStripe(stripe);
              return backupResult;
            }));
  }

  private Stream<BackupResult> sendRequestToAllServers(List<Server> servers, BackupExecutionRequest backupExecutionRequest) {
    return servers.parallelStream()
        .map(server -> threadPool.submit(() -> this.sendRequestToServer(server,
            s -> hostPortDiagnosticsMap.get(s.getHostPort()).getDiagnostics().invoke(DIAGNOSTIC_NAME, backupExecutionRequest.getCmdName()))))
        .collect(Collectors.toList())
        .parallelStream()
        .map(future -> {
          try {
            return future.get();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        })
        .flatMap(serverRespMap -> serverRespMap.entrySet().stream().map(serverRespEntry -> {
          try {
            BackupExecutionResult backupExecutionResult = BackupExecutionResult.valueOf(serverRespEntry.getValue());
            return new BackupResult(serverRespEntry.getKey(), backupExecutionResult);
          } catch (IllegalArgumentException e) {
            if (serverRespEntry.getValue().toLowerCase().contains("timeout")) {
              throw new IllegalStateException("Aborting backup as the connection timed out during the backup process.");
            }
            throw new IllegalStateException("Aborting backup due to server side errors. Please inspect server side logs for more details.");
          }
        }));
  }


  private Map<Server, String> sendRequestToServer(Server server, Function<Server, String> func) {
    if (hostPortDiagnosticsMap.get(server.getHostPort()) == null) {
      this.backupMsg(server, "TIMEOUT");
      return Collections.singletonMap(server, BackupExecutionResult.BACKUP_TIMEOUT.name());
    }

    try {
      String resp = func.apply(server);
      this.backupMsg(server, resp);
      return Collections.singletonMap(server, resp);
    } catch (RuntimeException e) {
      if (contains(e, TimeoutException.class)) {
        this.backupMsg(server, "TIMEOUT");
        return Collections.singletonMap(server, BackupExecutionResult.BACKUP_TIMEOUT.name());
      }
      throw e;
    }
  }

  private Stream<BackupResult> sendBackupNameToAllServers(List<Server> servers, String uniqueBackupName, Map<Server, String> serverStripeNameMap) {
    return servers.parallelStream()
        .map(server -> threadPool.submit(() -> this.sendRequestToServer(server,
            s -> hostPortDiagnosticsMap.get(s.getHostPort()).getDiagnostics().set(DIAGNOSTIC_NAME, "UniqueBackupName", String.format("%s:%s", uniqueBackupName, serverStripeNameMap.get(s))))))
        .collect(Collectors.toList())
        .parallelStream()
        .map(future -> {
          try {
            return future.get();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        })
        .flatMap(serverRespMap -> serverRespMap.entrySet().stream().map(serverRespEntry -> {
          return new BackupResult(serverRespEntry.getKey(),
              serverRespEntry.getValue().equals("SUCCESS") ? BackupExecutionResult.SUCCESS : (serverRespEntry.getValue().equals("BACKUP_TIMEOUT") ? BackupExecutionResult.BACKUP_TIMEOUT : BackupExecutionResult.BACKUP_FAILURE));
        }));
  }

  private Map<Server, String> createServerStripeNameMapping(Cluster cluster) {
    Map<Server, String> serverStripeNameMap = new HashMap<>();

    // Loop over all the stripes in index order and create the mapping.
    for (int i = 0; i < cluster.getStripes().size(); i++) {
      String stripeName = "stripe" + i;
      cluster.getStripes().get(i).getServers().forEach(server -> serverStripeNameMap.put(server, stripeName));
    }
    return serverStripeNameMap;
  }

  private void backupMsg(Server server, String suffixMsg) {
    if (suffixMsg.contains("Invalid JMX")) {
      LOGGER.info("{}: {}", server.getHostPort(), "Backup may not be configured on the server.");
      return;
    }

    if (suffixMsg.contains("Exception")) {
      LOGGER.info("{}: {}", server.getHostPort(), "Another backup may be in progress on the server.");
      return;
    }

    LOGGER.info("{}: {}", server.getHostPort(), suffixMsg);
  }

  private static class BackupResult {
    private final Server server;
    private final BackupExecutionResult backupExecutionResult;
    private volatile Stripe stripe;

    public BackupResult(Server server, BackupExecutionResult backupExecutionResult) {
      this.server = server;
      this.backupExecutionResult = backupExecutionResult;
    }

    public void setStripe(Stripe stripe) {
      this.stripe = stripe;
    }

    public Stripe getStripe() {
      return stripe;
    }

    public Server getServer() {
      return server;
    }

    public BackupExecutionResult getBackupExecutionResult() {
      return backupExecutionResult;
    }
  }
}
