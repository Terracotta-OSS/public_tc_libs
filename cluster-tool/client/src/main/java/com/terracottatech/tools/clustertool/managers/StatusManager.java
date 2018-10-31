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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tc.util.Conversion;
import com.terracottatech.tools.client.TopologyEntityProvider;
import com.terracottatech.tools.command.OutputFormat;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;
import com.terracottatech.tools.util.DetailedServerStateUtil;
import com.terracottatech.tools.util.TableFormatter;
import com.terracottatech.utilities.HostAndIpValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.terracottatech.tools.config.Stripe.ConfigType.AUDIT_DIRECTORY;
import static com.terracottatech.tools.config.Stripe.ConfigType.AUTHENTICATION;
import static com.terracottatech.tools.config.Stripe.ConfigType.BACKUP_RESTORE;
import static com.terracottatech.tools.config.Stripe.ConfigType.DATA_DIRECTORIES;
import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;
import static com.terracottatech.tools.config.Stripe.ConfigType.SSL_TLS;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST_DEPRECATED;
import static com.terracottatech.tools.detailed.state.LogicalServerState.UNREACHABLE;


public class StatusManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatusManager.class);
  private final CommonEntityManager entityManager;
  private String securityRootDirectory;

  public StatusManager() {
    this(new CommonEntityManager());
  }

  public StatusManager(CommonEntityManager entityManager) {
    this.entityManager = entityManager;
  }

  public void setSecurityRootDirectory(String securityRootDirectory) {
    this.securityRootDirectory = securityRootDirectory;
  }

  public String getServerStatus(String hostPort) {
    LOGGER.debug("getServerStatus called with: {}", hostPort);
    String status;
    try {
      status = invokeMBeanForServerStatus(hostPort);
    } catch (RuntimeException e) {
      LOGGER.debug("Invoking MBean for server status resulted in: {}", e);
      status = UNREACHABLE.name();
    }
    return status;
  }

  public int doClusterStatus(OutputFormat outputFormat, List<Stripe> stripes, ClusterConfiguration configuration) {
    switch (outputFormat) {
      case JSON:
        return doClusterStatusForJsonFormat(stripes, configuration);
      case TABULAR:
        return doClusterStatusForTabularFormat(stripes, configuration);
      default:
        throw new AssertionError("Unknown output format: " + outputFormat);
    }
  }

  public int doServersStatus(OutputFormat outputFormat, List<String> hostPortList) {
    switch (outputFormat) {
      case JSON:
        return doServersStatusForJsonFormat(hostPortList);
      case TABULAR:
        return doServersStatusForTabularFormat(hostPortList);
      default:
        throw new AssertionError("Unknown output format: " + outputFormat);
    }
  }

  private int doClusterStatusForJsonFormat(List<Stripe> stripes, ClusterConfiguration configuration) {
    Map<String, Object> finalObject = new LinkedHashMap<>();
    Map<String, Object> clusterConfiguration = sanitizeKeysForJson(addConfigurationInfo(configuration));
    finalObject.put("cluster-configuration", clusterConfiguration);

    AtomicInteger errorCount = new AtomicInteger(0);
    List<Map<String, Map<String, String>>> stripeList = new ArrayList<>();
    initializeListWithNulls(stripeList, stripes.size());

    stripes.parallelStream().forEach(stripe -> {
      Map<String, Map<String, String>> serverMap = new LinkedHashMap<>();
      List<Server> servers = stripe.getServers();

      servers.parallelStream().forEach(server -> {
        String hostPort = server.getHostPort();
        String serverStatus = getServerStatus(hostPort);
        if (serverStatus.equals(UNREACHABLE.name())) {
          errorCount.incrementAndGet();
        }
        Map<String, String> serverProperties = new LinkedHashMap<>();
        serverProperties.put("server-name", server.getName());
        serverProperties.put("status", serverStatus);
        serverMap.put(hostPort, serverProperties);
      });
      stripeList.set(stripes.indexOf(stripe), serverMap);
    });

    finalObject.put("stripes-information", stripeList);
    jsonize(finalObject);
    return errorCount.intValue();
  }

  private int doClusterStatusForTabularFormat(List<Stripe> stripes, ClusterConfiguration configuration) {
    List<List<List<String>>> stripeList = new ArrayList<>();
    initializeListWithNulls(stripeList, stripes.size());

    AtomicInteger errorCount = new AtomicInteger(0);
    List<String> headerList = new ArrayList<>(Arrays.asList("Server Name", "Host-Port", "Status"));
    stripes.parallelStream().forEach(stripe -> {
      List<List<String>> rowsList = new ArrayList<>();
      List<Server> servers = stripe.getServers();
      initializeListWithNulls(rowsList, servers.size());

      servers.parallelStream().forEach(server -> {
        String hostPort = server.getHostPort();
        String serverStatus = getServerStatus(hostPort);
        if (serverStatus.equals(UNREACHABLE.name())) {
          errorCount.incrementAndGet();
        }
        rowsList.set(servers.indexOf(server), Arrays.asList(server.getName(), hostPort, serverStatus));
      });
      stripeList.set(stripes.indexOf(stripe), rowsList);
    });

    Map<String, Object> objectMap = addConfigurationInfo(configuration);
    objectMap.forEach((property, value) -> LOGGER.info(property + ": {}", value));
    LOGGER.info(TableFormatter.NEW_LINE);

    for (int index = 0; index < stripes.size(); index++) {
      LOGGER.info("| STRIPE: {} |", index + 1);
      LOGGER.info(TableFormatter.formatAsTable(headerList, stripeList.get(index)));
    }
    return errorCount.intValue();
  }

  private int doServersStatusForJsonFormat(List<String> hostPortList) {
    Map<String, Map<String, String>> serversMap = new LinkedHashMap<>();
    AtomicInteger errorCount = new AtomicInteger(0);
    hostPortList.parallelStream().forEach(hostPort -> {
      Map<String, String> serverProperties = sanitizeKeysForJson(doStatusInternal(errorCount, hostPort));
      serversMap.put(serverProperties.remove("host-port"), serverProperties);
    });

    jsonize(serversMap);
    return errorCount.intValue();
  }

  private int doServersStatusForTabularFormat(List<String> hostPortList) {
    List<List<String>> rowsList = new CopyOnWriteArrayList<>();
    List<String> headerList = new ArrayList<>(Arrays.asList("Host-Port", "Status", "Member of Cluster", "Additional Information"));

    AtomicInteger errorCount = new AtomicInteger(0);
    hostPortList.parallelStream().forEach(hostPort -> {
      List<String> strings = new ArrayList<>(doStatusInternal(errorCount, hostPort).values());
      rowsList.add(strings);
    });

    LOGGER.info(TableFormatter.formatAsTable(headerList, rowsList));
    return errorCount.intValue();
  }

  private Map<String, String> doStatusInternal(AtomicInteger errorCount, String hostPort) {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Future<?>> futures = new ArrayList<>();
    futures.add(executorService.submit(() -> invokeMBeanForServerStatus(hostPort)));
    futures.add(executorService.submit(() -> getClusterConfiguration(hostPort)));

    Set<String> infoSet = new LinkedHashSet<>();
    List<Object> results = getFuturesResult(futures);
    shutdownAndAwaitTermination(executorService);

    String serverStatus = UNREACHABLE.name();
    Object result = results.get(0);
    if (result instanceof Throwable) {
      infoSet.add(getRootCause((Throwable) result));
      errorCount.incrementAndGet();
    } else if (result instanceof String) {
      serverStatus = (String) result;
    }

    ClusterConfiguration clusterConfiguration;
    String clusterName = "-";
    result = results.get(1);
    if (result instanceof Throwable) {
      infoSet.add(getRootCause((Throwable) result));
      errorCount.incrementAndGet();
    } else if (result instanceof ClusterConfiguration) {
      clusterConfiguration = (ClusterConfiguration) result;
      int lastIndexOfColon = hostPort.lastIndexOf(":");
      String hostPortToMatch = hostPort;
      if (lastIndexOfColon == -1 || HostAndIpValidator.isValidIPv6(hostPort, true)) {
        hostPortToMatch = hostPort + ":" + Server.DEFAULT_PORT;
      }
      String finalHostPortToMatch = hostPortToMatch;
      boolean noMatchForInputServer = clusterConfiguration.getCluster()
          .hostPortList()
          .stream()
          .noneMatch(hostName -> hostName.equals(finalHostPortToMatch));
      if (noMatchForInputServer) {
        infoSet.add("No match found for '" + finalHostPortToMatch + "' in known configuration");
        infoSet.add("Available servers are: " + clusterConfiguration.getCluster().hostPortList());
      }

      if (clusterConfiguration.getClusterName() != null) {
        clusterName = clusterConfiguration.getClusterName();
      } else {
        infoSet.add("Server is not part of a configured cluster");
      }
    }

    Map<String, String> serverProperties = new LinkedHashMap<>();
    serverProperties.put("Host-Port", hostPort);
    serverProperties.put("Status", serverStatus);
    serverProperties.put("Member of Cluster", clusterName);
    serverProperties.put("Additional Information", "-");
    if (!infoSet.isEmpty()) {
      serverProperties.put("Additional Information", String.join(". ", infoSet));
    }
    return serverProperties;
  }

  private static void initializeListWithNulls(List<?> list, int size) {
    for (int index = 0; index < size; index++) {
      list.add(null);
    }
  }

  private String invokeMBeanForServerStatus(String hostPort) {
    DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity closeableDiagnosticsEntity = null;
    String status;
    try {
      closeableDiagnosticsEntity = entityManager.getDiagnosticsEntity(hostPort, securityRootDirectory);
      status = DetailedServerStateUtil.getDetailedServerState(closeableDiagnosticsEntity.getDiagnostics());
    } finally {
      entityManager.closeEntity(closeableDiagnosticsEntity);
    }
    return status;
  }

  private static Map<String, Object> addConfigurationInfo(ClusterConfiguration configuration) {
    Map<String, Object> keyValuePairs = new LinkedHashMap<>();
    keyValuePairs.put("Cluster name", configuration.getClusterName());
    List<Stripe> stripes = configuration.getCluster().getStripes();
    keyValuePairs.put("Number of stripes", stripes.size());
    keyValuePairs.put("Total number of servers", configuration.getCluster().getServers().size());

    keyValuePairs.put("Total configured offheap", Conversion.memoryBytesAsSize(stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(OFFHEAP))
        .mapToLong(value -> (Long) value.getValue())
        .sum()).toUpperCase());
    keyValuePairs.put("Data directories configured", stripes.get(0).getConfigs().stream()
        .filter(config -> config.getType().equals(DATA_DIRECTORIES) && !config.getValue().equals("true"))
        .map(Stripe.Config::getName)
        .collect(Collectors.joining(", ")));
    keyValuePairs.put("Backup configured", stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .anyMatch(config -> config.getType().equals(BACKUP_RESTORE)));
    keyValuePairs.put("Security configured", getSecurityConfig(stripes));

    return keyValuePairs;
  }

  private static String getSecurityConfig(List<Stripe> stripes) {
    boolean auditing = stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .anyMatch(config -> config.getType().equals(AUDIT_DIRECTORY));
    boolean ssl = stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .anyMatch(config -> config.getType().equals(SSL_TLS));
    String authenticationType = stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(AUTHENTICATION))
        .map(Stripe.Config::getName)
        .findAny()
        .orElse(null);
    boolean whitelist = stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .anyMatch(config -> config.getType().equals(WHITELIST));
    boolean deprecatedWhitelist = stripes.stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .anyMatch(config -> config.getType().equals(WHITELIST_DEPRECATED));

    if (!ssl && authenticationType == null && !whitelist && !deprecatedWhitelist) {
      return "false";
    }

    Set<String> toReturn = new LinkedHashSet<>();
    if (auditing) {
      toReturn.add(AUDIT_DIRECTORY.getName());
    }

    if (ssl) {
      toReturn.add(SSL_TLS.getName());
    }

    if (authenticationType != null) {
      toReturn.add(AUTHENTICATION.getName() + " (" + authenticationType + ")");
    }

    if (whitelist) {
      toReturn.add(WHITELIST.getName());
    }

    if (deprecatedWhitelist) {
      toReturn.add(WHITELIST_DEPRECATED.getName());
    }

    return String.join(", ", toReturn);
  }

  private ClusterConfiguration getClusterConfiguration(String hostPort) {
    LOGGER.debug("getClusterConfiguration called with: {}", hostPort);
    TopologyEntityProvider.ConnectionCloseableTopologyEntity closeableTopologyEntity = null;
    ClusterConfiguration clusterConfiguration;
    try {
      closeableTopologyEntity = entityManager.getTopologyEntity(Collections.singletonList(hostPort), securityRootDirectory);
      clusterConfiguration = closeableTopologyEntity.getTopologyEntity().getClusterConfiguration();
    } finally {
      entityManager.releaseTopologyEntity(closeableTopologyEntity);
    }
    return clusterConfiguration;
  }

  private static String getRootCause(Throwable e) {
    Throwable cause;
    Throwable result = e;

    while ((cause = result.getCause()) != null && result != cause) {
      result = cause;
    }
    return result.getMessage();
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

  private <T> Map<String, T> sanitizeKeysForJson(Map<String, T> input) {
    Map<String, T> sanitizedMap = new LinkedHashMap<>();
    input.forEach((k, v) -> sanitizedMap.put(k.replace(" ", "-").toLowerCase(), v));
    return sanitizedMap;
  }

  private <T> void jsonize(Map<String, T> serversMap) {
    try {
      LOGGER.info(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(serversMap));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
