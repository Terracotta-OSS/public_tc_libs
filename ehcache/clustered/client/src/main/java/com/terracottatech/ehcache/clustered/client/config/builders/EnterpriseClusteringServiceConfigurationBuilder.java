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
package com.terracottatech.ehcache.clustered.client.config.builders;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.Builder;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.ehcache.clustered.client.config.ClusteringServiceConfiguration.DEFAULT_AUTOCREATE;

/**
 * A builder of enterprise {@link ClusteringServiceConfiguration ClusteringService configurations}.
 */
public class EnterpriseClusteringServiceConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final ConnectionSource connectionSource;
  private final Timeouts timeouts;
  private final Boolean autoCreate;
  private final Properties properties;

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param clusterUri cluster URI
   *
   * @return a clustering service configuration builder
   */
  public static EnterpriseClusteringServiceConfigurationBuilder enterpriseCluster(URI clusterUri) {
    return new EnterpriseClusteringServiceConfigurationBuilder(new ConnectionSource.ClusterUri(clusterUri), buildTimeout(),
        DEFAULT_AUTOCREATE, new Properties());
  }

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param servers an {@code Iterable} of {@code InetSocketAddress}es identifying the servers in the cluster
   * @param clusterTierManager cluster tier manager identifier
   *
   * @return a clustering service configuration builder
   */
  public static EnterpriseClusteringServiceConfigurationBuilder enterpriseCluster(Iterable<InetSocketAddress> servers,
                                                                                  String clusterTierManager) {
    return new EnterpriseClusteringServiceConfigurationBuilder(new ConnectionSource.ServerList(servers, clusterTierManager),
        buildTimeout(), DEFAULT_AUTOCREATE, new Properties());
  }

  /**
   * Creates a new builder connecting to the given cluster secured using SSL/TLS.
   *
   * @param clusterUri cluster URI
   * @param securityRootDirectory path to a directory containing security configuration information. The contents of the
   *                              directory must follow the well-defined structure.
   * @return a clustering service configuration builder
   */
  public static EnterpriseClusteringServiceConfigurationBuilder enterpriseSecureCluster(URI clusterUri, Path securityRootDirectory) {
    return new EnterpriseClusteringServiceConfigurationBuilder(new ConnectionSource.ClusterUri(clusterUri), buildTimeout(),
        DEFAULT_AUTOCREATE, getPropertiesWithSecurityRootDirectory(securityRootDirectory));
  }

  /**
   * Creates a new builder connecting to the given cluster.
   *
   * @param servers an {@code Iterable} of {@code InetSocketAddress}es identifying the servers in the cluster
   * @param clusterTierManager cluster tier manager identifier
   * @param securityRootDirectory path to a directory containing security configuration information. The contents of the
   *                              directory must follow the well-defined structure.
   *
   * @return a clustering service configuration builder
   */
  public static EnterpriseClusteringServiceConfigurationBuilder enterpriseSecureCluster(Iterable<InetSocketAddress> servers,
                                                                                        String clusterTierManager, Path securityRootDirectory) {
    return new EnterpriseClusteringServiceConfigurationBuilder(new ConnectionSource.ServerList(servers, clusterTierManager),
        buildTimeout(), DEFAULT_AUTOCREATE, getPropertiesWithSecurityRootDirectory(securityRootDirectory));
  }

  private EnterpriseClusteringServiceConfigurationBuilder(ConnectionSource connectionSource, Timeouts timeouts, boolean autoCreate, Properties properties) {
    this.connectionSource = connectionSource;
    this.timeouts = Objects.requireNonNull(timeouts, "Timeouts can't be null");
    this.autoCreate = autoCreate;
    this.properties = properties;
  }

  /**
   * Support connection to an existing entity or create if the entity if absent.
   *
   * @return a clustering service configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder autoCreate() {
    return serverSideConfigurationBuilder(true);
  }

  /**
   * Only support connection to an existing entity.
   *
   * @return a clustering service configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder expecting() {
    return serverSideConfigurationBuilder(false);
  }

  /**
   * Adds timeouts.
   * Read operations which time out return a result comparable to a cache miss.
   * Write operations which time out won't do anything.
   * Lifecycle operations which time out will fail with exception
   *
   * @param timeouts the amount of time permitted for all operations
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code timeouts} is {@code null}
   */
  public EnterpriseClusteringServiceConfigurationBuilder timeouts(Timeouts timeouts) {
    return new EnterpriseClusteringServiceConfigurationBuilder(this.connectionSource, timeouts, this.autoCreate, this.properties);
  }

  /**
   * Adds timeouts.
   * Read operations which time out return a result comparable to a cache miss.
   * Write operations which time out won't do anything.
   * Lifecycle operations which time out will fail with exception
   *
   * @param timeoutsBuilder the builder for amount of time permitted for all operations
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code timeouts} is {@code null}
   */
  public EnterpriseClusteringServiceConfigurationBuilder timeouts(Builder<? extends Timeouts> timeoutsBuilder) {
    return timeouts(timeoutsBuilder.build());
  }

  /**
   * Adds a read operation timeout.  Read operations which time out return a result comparable to
   * a cache miss.
   *
   * @param duration the amount of time permitted for read operations
   * @param unit the time units for {@code duration}
   *
   * @return a clustering service configuration builder
   *
   * @throws NullPointerException if {@code unit} is {@code null}
   * @throws IllegalArgumentException if {@code amount} is negative
   *
   * @deprecated Use {@link #timeouts(Timeouts)}. Note that calling this method will override any timeouts previously set
   * by setting the read operation timeout to the specified value and everything else to its default.
   */
  @Deprecated
  public EnterpriseClusteringServiceConfigurationBuilder readOperationTimeout(long duration, TimeUnit unit) {
    Duration readTimeout = Duration.of(duration, toChronoUnit(unit));
    return timeouts(TimeoutsBuilder.timeouts().read(readTimeout).build());
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return build(null);
  }

  /**
   * Internal method to build a new {@link ClusteringServiceConfiguration} from the {@link ServerSideConfigurationBuilder}.
   *
   * @param serverSideConfiguration the {@code ServerSideConfiguration} to use
   *
   * @return a new {@code ClusteringServiceConfiguration} instance built from {@code this}
   *        {@code ClusteringServiceConfigurationBuilder} and the {@code serverSideConfiguration} provided
   */
  ClusteringServiceConfiguration build(ServerSideConfiguration serverSideConfiguration) {
    return new ClusteringServiceConfiguration(connectionSource, timeouts, autoCreate, serverSideConfiguration, properties);
  }

  private static ChronoUnit toChronoUnit(TimeUnit unit) {
    if(unit == null) {
      return null;
    }
    switch (unit) {
      case NANOSECONDS:  return ChronoUnit.NANOS;
      case MICROSECONDS: return ChronoUnit.MICROS;
      case MILLISECONDS: return ChronoUnit.MILLIS;
      case SECONDS:      return ChronoUnit.SECONDS;
      case MINUTES:      return ChronoUnit.MINUTES;
      case HOURS:        return ChronoUnit.HOURS;
      case DAYS:         return ChronoUnit.DAYS;
      default: throw new AssertionError("Unknown unit: " + unit);
    }
  }

  private static Properties getPropertiesWithSecurityRootDirectory(Path securityRootDirectory) {
    Properties properties = new Properties();
    properties.put(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootDirectory.toString());
    return properties;
  }

  private EnterpriseServerSideConfigurationBuilder serverSideConfigurationBuilder(boolean autoCreate) {
    return new EnterpriseServerSideConfigurationBuilder(clusteringServiceConfigurationBuilder(timeouts, autoCreate));
  }

  private EnterpriseClusteringServiceConfigurationBuilder clusteringServiceConfigurationBuilder(Timeouts timeouts, boolean autoCreate) {
    return new EnterpriseClusteringServiceConfigurationBuilder(connectionSource, timeouts, autoCreate, this.properties);
  }

  private static Timeouts buildTimeout() {
    return TimeoutsBuilder.timeouts().build();
  }
}
