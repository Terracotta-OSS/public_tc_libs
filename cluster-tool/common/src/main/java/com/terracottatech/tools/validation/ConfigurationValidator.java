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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.util.Conversion;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.terracottatech.tools.config.Stripe.ConfigType.AUTHENTICATION;
import static com.terracottatech.tools.config.Stripe.ConfigType.DATA_DIRECTORIES;
import static com.terracottatech.tools.config.Stripe.ConfigType.FAILOVER_PRIORITY;
import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

/**
 * Perform cluster configuration validations.
 */
public class ConfigurationValidator implements Validator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationValidator.class);

  @Override
  public void validate(Cluster cluster, String messageFragment) throws IllegalArgumentException {
    LOGGER.debug("validate called with: {}, {}", cluster, messageFragment);
    if (cluster == null || messageFragment == null) {
      throw new NullPointerException("Cluster or messageFragment cannot be null.");
    }

    //Ensure that stripes configs are consistent with each other
    ensureConsistentConfigTypes(cluster, messageFragment);
    ensureConsistentSecurityConfiguration(cluster, messageFragment);
    ensureConsistentFailoverPriority(cluster, messageFragment);
    ensureConsistentOffHeap(cluster, messageFragment);
    ensureConsistentDataDirectories(cluster, messageFragment);
  }

  @Override
  public void validateStripeAgainstCluster(Stripe startupStripe, Cluster cluster) {
    LOGGER.debug("validateStripeAgainstCluster called with: {}, {}", startupStripe, cluster);
    validateAgainst(new Cluster(singletonList(startupStripe)), cluster, true);

    List<Server> startupServers = startupStripe.getServers();
    if (startupStripe.shouldValidateStrictly() && cluster.getStripes().stream().noneMatch(stripeMatch(startupServers))) {
      StringBuilder builder = new StringBuilder("Mismatched <server> elements. Server configuration lists the following servers: \n[");
      builder.append(startupServers.stream().map(Object::toString).collect(joining(", ")))
          .append("]\nbut no match found in the recorded cluster configuration\n");
      builder.append(cluster.getStripes().stream()
          .map(stripe -> stripe.getServers().stream().map(Object::toString).collect(joining(", ", "[", "]")))
          .collect(joining(", ", "[", "]")));

      throw new IllegalArgumentException(builder.toString());
    }
  }

  @Override
  public void validateAgainst(Cluster newCluster, Cluster configuredCluster) throws IllegalArgumentException {
    validateAgainst(newCluster, configuredCluster, false);
  }

  private void validateAgainst(Cluster newCluster, Cluster configuredCluster, boolean strict) throws IllegalArgumentException {
    LOGGER.debug("validateAgainst strict: {} called with: {}, {}", strict, newCluster, configuredCluster);
    if (newCluster == null || configuredCluster == null) {
      throw new NullPointerException("Clusters cannot be null.");
    }

    //Ensure that the new configs are the same as the configured cluster configs
    ensureConsistentConfigTypes(newCluster, configuredCluster, strict);
    ensureConsistentSecurityConfiguration(newCluster, configuredCluster, strict);
    ensureConsistentFailoverPriority(newCluster, configuredCluster, strict);
    ensureConsistentOffHeap(newCluster, configuredCluster, strict);
    ensureConsistentDataDirectories(newCluster, configuredCluster, strict);
  }

  private void ensureConsistentConfigTypes(Cluster cluster, String messageFragment) throws IllegalArgumentException {
    // Config Type present per stripes
    List<Set<String>> configTypesPerStripe = cluster.getStripes()
        .stream()
        .map(stripe -> stripe.getConfigs()
            .stream()
            .map(config -> config.getType().getName())
            .collect(Collectors.toSet()))
        .collect(Collectors.toList());

    // If all stripe has all config types then just return.
    if (new HashSet<>(configTypesPerStripe).size() == 1) {
      return;
    }

    throw new IllegalArgumentException("Mismatched config types in " + messageFragment + ": [" + configTypesPerStripe.stream().map(Object::toString).collect(joining(", ")) + "]");
  }

  private void ensureConsistentConfigTypes(Cluster newCluster, Cluster configuredCluster, boolean strict) throws IllegalArgumentException {
    Set<String> newAllConfigTypes = getAllConfigTypes(newCluster);
    Set<String> configuredAllConfigTypes = getAllConfigTypes(configuredCluster);
    boolean valid;

    if (strict) {
      valid = newAllConfigTypes.equals(configuredAllConfigTypes);
    } else {
      valid = newAllConfigTypes.containsAll(configuredAllConfigTypes);
    }

    if (!valid) {
      throw new IllegalArgumentException("Mismatched config types. Provided: " + newAllConfigTypes + ", but previously known: " + configuredAllConfigTypes);
    }
  }

  private void ensureConsistentOffHeap(Cluster cluster, String messageFragment) throws IllegalArgumentException {
    // Identify all off heap configs
    Set<Stripe.Config<?>> uniqueOffHeapConfigs = getUniqueOffHeapConfigs(cluster);

    // If all stripes have same off heap configuration for all the resources then just return.
    Optional<Stripe> mismatchOffHeapStripe = cluster.getStripes().stream()
        .filter(stripe -> !stripe.getConfigs()
            .stream()
            .filter(config -> config.getType().equals(OFFHEAP))
            .collect(Collectors.toSet())
            .containsAll(uniqueOffHeapConfigs))
        .findFirst();

    if (!mismatchOffHeapStripe.isPresent()) {
      return;
    }

    // Off Heap resource configuration per stripe
    List<List<String>> offHeapConfigsPerStripe = cluster.getStripes().stream()
        .map(stripe -> stripe.getConfigs().stream()
            .filter(config -> config.getType().equals(OFFHEAP))
            .map(this::getNameValue)
            .collect(Collectors.toList()))
        .collect(Collectors.toList());

    throw new IllegalArgumentException("Mismatched off-heap resources in " + messageFragment + ": " + offHeapConfigsPerStripe);
  }

  private void ensureConsistentOffHeap(Cluster newCluster, Cluster configuredCluster, boolean strict) {
    Set<Stripe.Config<?>> newUniqueOffHeapConfigs = getUniqueOffHeapConfigs(newCluster);
    Set<Stripe.Config<?>> configuredUniqueOffHeapConfigs = getUniqueOffHeapConfigs(configuredCluster);

    boolean valid;
    if (strict) {
      valid = newUniqueOffHeapConfigs.equals(configuredUniqueOffHeapConfigs);
    } else {
      valid = newUniqueOffHeapConfigs.containsAll(configuredUniqueOffHeapConfigs);
    }

    if (!valid) {
      throw new IllegalArgumentException("Mismatched off-heap resources. Provided: " +
          newUniqueOffHeapConfigs.stream().map(this::getNameValue).collect(Collectors.toList()) +
          " , but previously known: " + configuredUniqueOffHeapConfigs.stream().map(this::getNameValue).collect(Collectors.toList()));
    }

    Iterator<Stripe.Config<?>> newConfigIterator = newUniqueOffHeapConfigs.iterator();
    Iterator<Stripe.Config<?>> configuredConfigIterator = configuredUniqueOffHeapConfigs.iterator();

    while (newConfigIterator.hasNext() && configuredConfigIterator.hasNext()) {
      Stripe.Config<?> newOffheap = newConfigIterator.next();
      Long newValue = (Long) newOffheap.getValue();

      Stripe.Config<?> configuredOffheap = configuredConfigIterator.next();
      Long configuredValue = (Long) configuredOffheap.getValue();

      if (strict) {
        valid = newValue.equals(configuredValue);
      } else {
        valid = newValue >= configuredValue;
      }

      if (!valid) {
        throw new IllegalArgumentException("Mismatched off-heap resources. Provided: " +
            getNameValue(newOffheap) + ", but previously known: " + getNameValue(configuredOffheap));
      }
    }
  }

  private void ensureConsistentDataDirectories(Cluster cluster, String messageFragment) throws IllegalArgumentException {
    // Identify unique data directories
    Set<String> uniqueDataDirectories = getUniqueConfigNames(cluster, DATA_DIRECTORIES);

    // If all stripes have all the unique data directories then just return
    Optional<Stripe> mismatchedDataDirectoriesStripe = cluster.getStripes()
        .stream()
        .filter(stripe -> !stripe.getConfigs()
            .stream()
            .filter(config -> config.getType().equals(DATA_DIRECTORIES))
            .map(Stripe.Config::getName)
            .collect(Collectors.toSet())
            .containsAll(uniqueDataDirectories))
        .findFirst();

    if (!mismatchedDataDirectoriesStripe.isPresent()) {
      return;
    }

    // Data directories resource configuration per stripe
    List<List<String>> dataDirectoriesPerStripe = cluster.getStripes().stream()
        .map(stripe -> stripe.getConfigs().stream()
            .filter(config -> config.getType().equals(DATA_DIRECTORIES))
            .map(Stripe.Config::getName)
            .collect(Collectors.toList()))
        .collect(Collectors.toList());

    throw new IllegalArgumentException("Mismatched data directories in " + messageFragment + ": " + dataDirectoriesPerStripe);
  }

  private void ensureConsistentDataDirectories(Cluster newCluster, Cluster configuredCluster, boolean strict) {
    Set<String> newUniqueDataDirectories = getUniqueConfigNames(newCluster, DATA_DIRECTORIES);
    Set<String> configuredUniqueDataDirectories = getUniqueConfigNames(configuredCluster, DATA_DIRECTORIES);
    boolean valid;
    if (strict) {
      valid = newUniqueDataDirectories.equals(configuredUniqueDataDirectories);
    } else {
      valid = newUniqueDataDirectories.containsAll(configuredUniqueDataDirectories);
    }
    if (!valid) {
      throw new IllegalArgumentException("Mismatched data directories. Provided: " + newUniqueDataDirectories +
                                         ", but previously known: " + configuredUniqueDataDirectories);
    }
  }

  private void ensureConsistentSecurityConfiguration(Cluster cluster, String messageFragment) {
    Set<Stripe.Config<?>> configs = getUniqueConfigTypes(cluster, AUTHENTICATION);
    if (configs.size() > 1) {
      throw new IllegalArgumentException("Mismatched security configuration in " + messageFragment + ": " + configs);
    }
  }

  private void ensureConsistentSecurityConfiguration(Cluster newCluster, Cluster configuredCluster, boolean strict) {
    Set<String> newConfigNames = getUniqueConfigNames(newCluster, AUTHENTICATION);
    Set<String> configuredConfigNames = getUniqueConfigNames(configuredCluster, AUTHENTICATION);
    if (newConfigNames.size() > 1) {
      throw new IllegalArgumentException("Mismatched security configuration: " + newConfigNames);
    }

    boolean valid;
    if (strict) {
      valid = newConfigNames.equals(configuredConfigNames);
    } else {
      valid = newConfigNames.containsAll(configuredConfigNames);
    }

    if (!valid) {
      throw new IllegalArgumentException("Mismatched security configuration. Provided: " + newConfigNames +
                                         ", but previously known: " + configuredConfigNames);
    }
  }

  private void ensureConsistentFailoverPriority(Cluster cluster, String messageFragment) {
    Set<Stripe.Config<?>> configs = getUniqueConfigTypes(cluster, FAILOVER_PRIORITY);
    if (configs.size() > 1) {
      throw new IllegalArgumentException("Mismatched failover priority in " + messageFragment + ": " + configs);
    }
  }

  private void ensureConsistentFailoverPriority(Cluster newCluster, Cluster configuredCluster, boolean strict) {
    Set<Stripe.Config<?>> newConfigs = getUniqueConfigTypes(newCluster, FAILOVER_PRIORITY);
    Set<Stripe.Config<?>> configuredConfigs = getUniqueConfigTypes(configuredCluster, FAILOVER_PRIORITY);
    if (newConfigs.size() > 1) {
      throw new IllegalArgumentException("Mismatched failover priority: " + newConfigs);
    }

    if (strict && !newConfigs.equals(configuredConfigs)) {
      throw new IllegalArgumentException("Mismatched failover priority. Provided: " + newConfigs +
                                         ", but previously known: " + configuredConfigs);
    }
  }

  private Set<String> getAllConfigTypes(Cluster cluster) {
    return cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .map(config -> config.getType().getName())
        .collect(Collectors.toSet());
  }

  private Set<Stripe.Config<?>> getUniqueOffHeapConfigs(Cluster cluster) {
    Comparator<Stripe.Config<?>> comparator = Comparator.comparing(Stripe.Config::getName);
    return cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(OFFHEAP))
        .collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
  }

  private Set<String> getUniqueConfigNames(Cluster cluster, Stripe.ConfigType configType) {
    return cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(configType))
        .map(Stripe.Config::getName)
        .collect(Collectors.toSet());
  }

  private Set<Stripe.Config<?>> getUniqueConfigTypes(Cluster cluster, Stripe.ConfigType configType) {
    return cluster.getStripes().stream()
        .flatMap(stripe -> stripe.getConfigs().stream())
        .filter(config -> config.getType().equals(configType))
        .collect(Collectors.toSet());
  }

  private String getNameValue(Stripe.Config<?> config) {
    return config.getName() + ": " + Conversion.memoryBytesAsSize((Long) config.getValue()).toUpperCase();
  }

  private Predicate<Stripe> stripeMatch(List<Server> startupServers) {
    return stripe -> {
      List<Server> servers = stripe.getServers();
      if (servers.size() != startupServers.size()) {
        return false;
      }

      for (int i = 0; i < servers.size(); i++) {
        Server server = servers.get(i);
        Server startupServer = startupServers.get(i);
        if (server.getName().contains("%") || startupServer.getName().contains("%")) {
          // If server name contains a substitutable parameter, only compare host and port
          if (!startupServer.getHostPort().equals(server.getHostPort())) {
            return false;
          }
        } else {
          if (!startupServer.equals(server)) {
            return false;
          }
        }
      }
      return true;
    };
  }
}
