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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.config.Builder;
import org.ehcache.config.units.MemoryUnit;

import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Constructs the server-side portion of a {@link ClusteringServiceConfiguration}.  An instance of this
 * class is used in conjunction with {@link EnterpriseClusteringServiceConfigurationBuilder} and is obtained from
 * the {@link EnterpriseClusteringServiceConfigurationBuilder#autoCreate() autoCreate} and
 * {@link EnterpriseClusteringServiceConfigurationBuilder#expecting() expecting} methods of that class.
 */
public class EnterpriseServerSideConfigurationBuilder implements Builder<ClusteringServiceConfiguration> {

  private final EnterpriseClusteringServiceConfigurationBuilder clientSideBuilder;
  private final String defaultServerResource;
  private final Map<String, Pool> pools;

  private final RestartableOffHeapMode offHeapMode;
  private final String logRoot;
  private final String frsIdentifier;


  EnterpriseServerSideConfigurationBuilder(EnterpriseClusteringServiceConfigurationBuilder clientSideBuilder) {
    if (clientSideBuilder == null) {
      throw new NullPointerException("clientSideBuilder can not be null");
    }
    this.clientSideBuilder = clientSideBuilder;
    this.defaultServerResource = null;
    this.pools = emptyMap();
    this.logRoot = null;
    this.offHeapMode = RestartableOffHeapMode.FULL;
    this.frsIdentifier = null;
  }

  private EnterpriseServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                   String defaultServerResource) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.pools = original.pools;
    this.logRoot = original.logRoot;
    this.offHeapMode = original.offHeapMode;
    this.frsIdentifier = original.frsIdentifier;

    this.defaultServerResource = defaultServerResource;
  }

  private EnterpriseServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original, String poolName,
                                                   Pool poolDefinition) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.defaultServerResource = original.defaultServerResource;
    this.logRoot = original.logRoot;
    this.offHeapMode = original.offHeapMode;
    this.frsIdentifier = original.frsIdentifier;

    Map<String, Pool> pools = new HashMap<>(original.pools);
    if (pools.put(poolName, poolDefinition) != null) {
      throw new IllegalArgumentException("Pool '" + poolName + "' already defined");
    }
    this.pools = unmodifiableMap(pools);
  }

  private EnterpriseServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                   String identifier,
                                                   IdentifierType identifierType) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.defaultServerResource = original.defaultServerResource;
    this.pools = original.pools;
    this.offHeapMode = original.offHeapMode;

    switch (identifierType) {
      case LOG_ROOT_IDENTIFIER:
        this.logRoot = identifier;
        this.frsIdentifier = original.frsIdentifier;
        break;
      case RESTART_IDENTIFIER:
        this.logRoot = original.logRoot;
        this.frsIdentifier = identifier;
        break;
      default:
        throw new AssertionError("Found unexpected IdentifierType. Accepted IdentifierTypes are 'LOG_ROOT_IDENTIFIER' and 'RESTART_IDENTIFIER'");
    }
  }

  private EnterpriseServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                   RestartableOffHeapMode offHeapMode) {
    this.clientSideBuilder = original.clientSideBuilder;
    this.defaultServerResource = original.defaultServerResource;
    this.pools = original.pools;
    this.logRoot = original.logRoot;
    this.frsIdentifier = original.frsIdentifier;

    this.offHeapMode = offHeapMode;
  }

  /**
   * Sets the default server resource for pools and caches.
   *
   * @param defaultServerResource default server resource
   *
   * @return a clustering server configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder defaultServerResource(String defaultServerResource) {
    return new EnterpriseServerSideConfigurationBuilder(this, defaultServerResource);
  }

  /**
   * Adds a resource pool with the given name and size and consuming the given server resource.
   *
   * @param name pool name
   * @param size pool size
   * @param unit pool size unit
   * @param serverResource server resource to consume
   *
   * @return a clustering tier server configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit, String serverResource) {
    return resourcePool(name, new Pool(unit.toBytes(size), serverResource));
  }

  /**
   * Adds a resource pool with the given name and size and consuming the default server resource.
   *
   * @param name pool name
   * @param size pool size
   * @param unit pool size unit
   *
   * @return a clustering tier server configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit) {
    return resourcePool(name, new Pool(unit.toBytes(size)));
  }

  /**
   * Adds a resource pool with the given name and definition
   *
   * @param name pool name
   * @param definition pool definition
   *
   * @return a clustering tier server configuration builder
   */
  public EnterpriseServerSideConfigurationBuilder resourcePool(String name, Pool definition) {
    return new EnterpriseServerSideConfigurationBuilder(this, name, definition);
  }

  /**
   * Adds a restart configuration.
   *
   * @param logRoot log root identifier for location of FRS logs.
   *
   * @return a clustering tier server configuration builder
   */
  public RestartableServerSideConfigurationBuilder restartable(final String logRoot) {
    checkValidIdentifier(logRoot, "Fast Restart Store Log Root Identifier");
    return new RestartableServerSideConfigurationBuilder(this, logRoot, IdentifierType.LOG_ROOT_IDENTIFIER);
  }

  static void checkValidIdentifier(String id, String idType) {
    if (id == null || id.trim().length() <= 0) {
      throw new IllegalArgumentException(idType + " cannot be null or empty");
    }
  }

  @Override
  public ClusteringServiceConfiguration build() {
    return clientSideBuilder.build(buildServerSideConfiguration());
  }

  private ServerSideConfiguration buildServerSideConfiguration() {
    final RestartConfiguration restartConfiguration =
        (logRoot != null) ?
            new RestartConfiguration(logRoot, offHeapMode, frsIdentifier) :
            null;
    if (defaultServerResource == null) {
      return (restartConfiguration != null) ?
          new EnterpriseServerSideConfiguration(pools, restartConfiguration) :
          new ServerSideConfiguration(pools);
    } else {
      return (restartConfiguration != null) ?
          new EnterpriseServerSideConfiguration(defaultServerResource, pools, restartConfiguration) :
          new ServerSideConfiguration(defaultServerResource, pools);
    }
  }

  private enum IdentifierType {
    LOG_ROOT_IDENTIFIER,
    RESTART_IDENTIFIER
  }

  /**
   * Constructs the restartable server-side portion of a {@link ClusteringServiceConfiguration}.
   */
  public static final class RestartableServerSideConfigurationBuilder extends EnterpriseServerSideConfigurationBuilder {

    RestartableServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                              String identifier,
                                              IdentifierType identifierType) {
      super(original, identifier, identifierType);
    }

    private RestartableServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                      RestartableOffHeapMode offHeapMode) {
      super(original, offHeapMode);
    }

    private RestartableServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                      String name,
                                                      Pool definition) {
      super(original, name, definition);
    }

    private RestartableServerSideConfigurationBuilder(EnterpriseServerSideConfigurationBuilder original,
                                                      String defaultServerResource) {
      super(original, defaultServerResource);
    }

    /**
     * Specifies the mode of the restartable offHeap.
     * <p>
     *   This is to specify the mode of a restartable offHeap. See {@link ClusteredRestartableResourcePoolBuilder}
     *   for how to set offHeap sizes.
     *
     * @return a clustering tier server configuration builder
     */
    public RestartableServerSideConfigurationBuilder withRestartableOffHeapMode(RestartableOffHeapMode offHeapMode) {
      return new RestartableServerSideConfigurationBuilder(this, offHeapMode);
    }

    /**
     * Specifies the {@code restartIdentifier} for this configuration.
     * <p>
     *   This identifier uniquely identifies a Cluster Tier Manager within the FRS log
     *
     * @return a clustering tier server configuration builder
     */
    public RestartableServerSideConfigurationBuilder withRestartIdentifier(String restartIdentifier) {
      checkValidIdentifier(restartIdentifier, "Fast Restart Store Restart Identifier");
      return new RestartableServerSideConfigurationBuilder(this, restartIdentifier, IdentifierType.RESTART_IDENTIFIER);
    }

    @Override
    public RestartableServerSideConfigurationBuilder defaultServerResource(String defaultServerResource) {
      return new RestartableServerSideConfigurationBuilder(this, defaultServerResource);
    }

    @Override
    public RestartableServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit, String serverResource) {
      return resourcePool(name, new Pool(unit.toBytes(size), serverResource));
    }

    @Override
    public RestartableServerSideConfigurationBuilder resourcePool(String name, long size, MemoryUnit unit) {
      return resourcePool(name, new Pool(unit.toBytes(size)));
    }

    @Override
    public RestartableServerSideConfigurationBuilder resourcePool(String name, Pool definition) {
      return new RestartableServerSideConfigurationBuilder(this, name, definition);
    }
  }
}
