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
package com.terracottatech.ehcache.common.frs.metadata.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.ehcache.common.frs.RestartLogLifecycle;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.ehcache.common.frs.exceptions.RestartLogCreationException;
import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.RootConfiguration;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.terracottatech.ehcache.common.frs.FrsCodecFactory.toByteBuffer;

/**
 * Generic implementation that handles a single metadata log containing multiple levels of config metadata.
 * <p>
 * This metadata is per container and controls an entire FRS container.
 * <p>
 * A container of FRS can contain the following:
 *    a metadata log. handled by {@code this} class.
 *    one or more data logs; the file location of these data logs is contained in the metadata.
 */
class PerContainerMetadataStorage<K1, V1 extends RootConfiguration, V2 extends ChildConfiguration<K1>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerContainerMetadataStorage.class);

  // location of metadata logs
  private static final String METADATA_DIRECTORY = "metadata";
  private static final String LEVEL1 = "l1-";
  private static final String LEVEL2 = "l2-";
  private static final String LEVELN = "ln-";

  // this is just a soft number for calculating the starting next index and not a 'hard' limit.
  private static final int MAX_OTHER_CHILD_PER_CHILD = 1000;

  private final RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> metadataObjectManager;
  private final File containerDir;
  private final Map<Class<? extends ChildConfiguration<String>>,
      RestartableGenericMap<String, ? extends ChildConfiguration<String>>> otherChildMapOfMaps;

  private final RestartLogLifecycle restartLogLifecycle;

  // various metadata maps
  private final RestartableGenericMap<K1, V1> parentConfigMap;
  private final RestartableGenericMap<String, V2> childConfigMap;

  // deleted config objects are never deleted for first and second level configs..
  // they are moved to a deleted map. helps retain history as well as avoids reuse of
  // object identifiers in the main data logs.
  private final RestartableGenericMap<K1, V1> deletedParentConfigMap;
  private final RestartableGenericMap<String, V2> deletedChildConfigMap;

  private int currentParentIndex = 0;
  private int currentChildIndex = 0;
  private int currentOtherChildIndex = 0;

  PerContainerMetadataStorage(File containerDir, Class<K1> parentKeyType, Class<V1> parentType, Class<V2> childType,
                              Set<Class<? extends ChildConfiguration<String>>> otherChildTypes) {
    LOGGER.info("Initializing and Starting metadata store for container " + containerDir.getAbsolutePath());
    this.metadataObjectManager = new RegisterableObjectManager<>();
    this.containerDir = containerDir;

    File metadataLogDir = new File(containerDir, METADATA_DIRECTORY);
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> metadataLog = createMetadataStore(metadataLogDir);

    restartLogLifecycle = new RestartLogLifecycle(metadataLogDir, metadataLog);

    parentConfigMap = new RestartableGenericMap<>(parentKeyType, parentType,
        toByteBuffer(LEVEL1 + parentType.getSimpleName()), metadataLog);
    metadataObjectManager.registerObject(parentConfigMap);

    childConfigMap = new RestartableGenericMap<>(String.class, childType,
        toByteBuffer(LEVEL2 + childType.getSimpleName()), metadataLog);
    metadataObjectManager.registerObject(childConfigMap);

    deletedParentConfigMap = new RestartableGenericMap<>(parentKeyType, parentType,
        toByteBuffer("deleted-" + LEVEL1 + parentType.getSimpleName()), metadataLog);
    metadataObjectManager.registerObject(deletedParentConfigMap);

    deletedChildConfigMap = new RestartableGenericMap<>(String.class, childType,
        toByteBuffer("deleted-" + LEVEL2 + childType.getSimpleName()), metadataLog);
    metadataObjectManager.registerObject(deletedChildConfigMap);

    this.otherChildMapOfMaps = new HashMap<>();
    for (Class<? extends ChildConfiguration<String>> otherChildType : otherChildTypes)
    {
      // load meta about all children further down in the hierarchy
      @SuppressWarnings("unchecked")
      RestartableGenericMap<String, ChildConfiguration<String>> otherChildMap =
          new RestartableGenericMap<>(String.class,
              (Class<ChildConfiguration<String>>) otherChildType,
              toByteBuffer(LEVELN + otherChildType.getSimpleName()), metadataLog);
      metadataObjectManager.registerObject(otherChildMap);
      otherChildMapOfMaps.put(otherChildType, otherChildMap);
    }
  }

  final void bootstrapAsync() {
    // start the metadata store asynchronously
    restartLogLifecycle.startStoreAsync();
  }

  Set<FrsDataLogIdentifier> bootstrapWait() {
    if (restartLogLifecycle == null) {
      // this cannot happen unless there is a programming error.
      throw new AssertionError("Internal Error : Restart log not bootstrapped yet");
    }
    restartLogLifecycle.waitOnCompletion();
    return updateLogsAndCurrentIndex();
  }

  /**
   * Shutdown the store.
   */
  void shutdown() {
    LOGGER.info("Shutting down metadata store for container " + containerDir.getAbsolutePath());
    restartLogLifecycle.shutStore();
  }

  synchronized boolean addRootConfiguration(K1 rootId, V1 newParentConfiguration) {
    boolean added = false;
    V1 existingConfig = parentConfigMap.putIfAbsent(rootId, newParentConfiguration);
    if (existingConfig != null) {
      // existing parent configuration..load it and validate config
      existingConfig.validate(newParentConfiguration);
      newParentConfiguration.setObjectIndex(existingConfig.getObjectIndex());
      parentConfigMap.put(rootId, newParentConfiguration);
    } else {
      newParentConfiguration.setObjectIndex(this.currentParentIndex);
      this.currentParentIndex++;
      added = true;
    }
    return added;
  }

  synchronized boolean addChildConfiguration(String composedId, V2 newChildConfiguration) {
    if (!parentConfigMap.containsKey(newChildConfiguration.getParentId())) {
      throw new IllegalStateException("Cannot add a child as parent with ID " + newChildConfiguration.getParentId()
                                      + "does not exist");
    }
    boolean added = false;
    newChildConfiguration.setObjectIndex(this.currentChildIndex);
    final V2 existingConfig = childConfigMap.putIfAbsent(composedId, newChildConfiguration);
    if (existingConfig != null) {
      // existing parent configuration..load it and validate config
      existingConfig.validate(newChildConfiguration);
    } else {
      this.currentChildIndex++;
      added = true;
    }
    return added;
  }

  synchronized  <V3 extends ChildConfiguration<String>> boolean addOtherChildConfiguration(String composedId,
                                                                             V3 newOtherChildConfiguration,
                                                                             Class<V3> childType) {
    @SuppressWarnings("unchecked")
    final ConcurrentMap<String, V3> otherChildMap = (ConcurrentMap<String, V3>)otherChildMapOfMaps.get(childType);
    if (otherChildMap == null) {
      throw new AssertionError("Internal Error: Child Type " + childType.getName() +
                               " not registered as metadata item during bootstrap");
    }
    boolean added = false;
    newOtherChildConfiguration.setObjectIndex(this.currentOtherChildIndex);
    V3 existingConfig = otherChildMap.putIfAbsent(composedId, newOtherChildConfiguration);
    if (existingConfig != null) {
      // existing configuration..validate it to be same
      existingConfig.validate(newOtherChildConfiguration);
    } else {
      this.currentOtherChildIndex++;
      added = true;
    }
    return added;
  }

  synchronized void removeRootConfiguration(K1 parentId) {
    final V1 configToRemove = parentConfigMap.remove(parentId);
    if (configToRemove != null) {
      removeAllChildConfiguration(parentId);
      deletedParentConfigMap.put(parentId, configToRemove);
    }
  }

  synchronized void removeChildConfiguration(String childConfigurationId) {
    final V2 configToRemove = childConfigMap.remove(childConfigurationId);
    if (configToRemove != null) {
      removeAllOtherChildConfiguration(childConfigurationId);
      deletedChildConfigMap.put(childConfigurationId, configToRemove);
    }
  }

  synchronized  <V3 extends ChildConfiguration<String>> void removeOtherChildConfiguration(String childConfigurationId,
                                                                            Class<V3> otherChildType) {
    @SuppressWarnings("unchecked")
    final ConcurrentMap<String, V3> otherChildMap = (ConcurrentMap<String, V3>)otherChildMapOfMaps.get(otherChildType);
    if (otherChildMap == null) {
      throw new AssertionError("Internal Error: Child Type " + otherChildType.getName() +
                               " not registered as metadata item during bootstrap");
    }
    final V3 configToRemove = otherChildMap.remove(childConfigurationId);
    if (configToRemove != null) {
      // remove all children for which this is parent
      removeAllOtherChildConfiguration(childConfigurationId);
    }
  }

  Map<K1, V1> getRootConfiguration() {
    return Collections.unmodifiableMap(parentConfigMap);
  }

  Map<String, V2> getChildConfigurationByDataLog(final String dataLogName) {
    return getFilteredMap(childConfigMap, savedConfig -> savedConfig.getDataLogIdentifier().getDataLogName().equals(dataLogName));
  }

  Map<String, V2> getChildConfigurationByRoot(final K1 parentId) {
    return getFilteredMap(childConfigMap, savedConfig -> savedConfig.getParentId().equals(parentId));
  }

  <V3 extends ChildConfiguration<String>> Map<String, V3> getOtherChildConfigurationByLog(final String dataLogName,
                                                                                          final Class<V3> childType) {
    @SuppressWarnings("unchecked")
    final ConcurrentMap<String, V3> otherChildMap = (ConcurrentMap<String, V3>)otherChildMapOfMaps.get(childType);
    if (otherChildMap == null) {
      throw new AssertionError("Internal Error: Child Type " + childType.getName() +
                               " not registered as metadata item during bootstrap");
    }
    return getFilteredMap(otherChildMap, savedConfig -> savedConfig.getDataLogIdentifier().getDataLogName().equals(dataLogName));
  }

  <V3 extends ChildConfiguration<String>> Map<String, V3> getOtherChildConfigurationByParent(final String parentId,
                                                                                             final Class<V3> childType) {
    @SuppressWarnings("unchecked")
    final ConcurrentMap<String, V3> otherChildMap = (ConcurrentMap<String, V3>)otherChildMapOfMaps.get(childType);
    if (otherChildMap == null) {
      throw new AssertionError("Internal Error: Child Type " + childType.getName() +
                               " not registered as metadata item during bootstrap");
    }
    return getFilteredMap(otherChildMap, savedConfig -> savedConfig.getParentId().equals(parentId));
  }

  <V3 extends ChildConfiguration<String>> V3 getOtherChildConfigurationById(String composedId, Class<V3> otherChildType) {
    @SuppressWarnings("unchecked")
    final ConcurrentMap<String, V3> otherChildMap = (ConcurrentMap<String, V3>)otherChildMapOfMaps.get(otherChildType);
    if (otherChildMap == null) {
      throw new AssertionError("Internal Error: Child Type " + otherChildType.getName() +
                               " not registered as metadata item during bootstrap");
    }
    return otherChildMap.get(composedId);
  }

  V2 getChildConfigurationById(String composedId) {
    return childConfigMap.get(composedId);
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createMetadataStore(final File logDirectory) {
    final boolean exists = logDirectory.mkdirs();
    LOGGER.info("{} store area for metadata at location {}", exists ? "Loading" : "Creating", logDirectory);
    final Properties properties = new Properties();
    try {
      return RestartStoreFactory.createStore(metadataObjectManager, logDirectory, properties);
    } catch (IOException | RestartStoreException e) {
      throw new RestartLogCreationException(logDirectory, exists, "Metadata Log", e);
    }
  }

  /**
   * Creates a unmodifiable filtered map containing filtered items from the original map.
   * <p>
   *  cannot use java8 streams here..as of now..do the traditional way.
   *
   * @param mapToFilter Original map that is filtered
   * @param testPredicate Predicate function that returns true, iff filter criteria passes
   * @param <K> Key type of the map
   * @param <V> Value type of the map
   * @return Unmodifiable map containing filtered items
   */
  private <K, V> Map<K, V> getFilteredMap(Map<K, V> mapToFilter, Predicate<V> testPredicate) {
    if (mapToFilter == null || mapToFilter.size() < 1) {
      return Collections.emptyMap();
    }
    final Map<K, V> newMap = new HashMap<>();
    for (Map.Entry<K, V> entry : mapToFilter.entrySet()) {
      if (testPredicate.test(entry.getValue())) {
        newMap.put(entry.getKey(), entry.getValue());
      }
    }
    return Collections.unmodifiableMap(newMap);
  }

  /**
   * Find the appropriate start index to use for actual data objects that are created in the data log.
   * This helps in ensuring that an identifier is never repeated in the data logs for objects that are
   * destroyed and recreated.
   * <p>
   * Note: This is a bit inefficient during startup, but it is assumed that the number of metadata objects
   * are relatively much much lower, compared to the actual data objects.
   */
  private synchronized Set<FrsDataLogIdentifier> updateLogsAndCurrentIndex() {
    int currentMax = getMaxAndConsumeMap(deletedParentConfigMap, 0, null);
    this.currentParentIndex = getMaxAndConsumeMap(parentConfigMap, currentMax, null) + 1;

    currentMax = getMaxAndConsumeMap(deletedChildConfigMap, 0, null);
    final Set<FrsDataLogIdentifier> dataLogDirs = new HashSet<>();
    currentMax = getMaxAndConsumeMap(childConfigMap, currentMax, v2 -> dataLogDirs.add(v2.getDataLogIdentifier()));
    this.currentChildIndex = currentMax + 1;

    currentMax = (this.currentChildIndex - 1) * MAX_OTHER_CHILD_PER_CHILD + 1;
    for (RestartableGenericMap<String, ? extends ChildConfiguration<?>> map : otherChildMapOfMaps.values()) {
      currentMax = getMaxAndConsumeMap(map, currentMax, null) + 1;
    }
    this.currentOtherChildIndex = currentMax;
    return dataLogDirs;
  }

  private <K, V extends MetadataConfiguration> int getMaxAndConsumeMap(RestartableGenericMap<K, V> restartableMap,
                                                                       int currentMax,
                                                                       Consumer<V> valueConsumer) {
    int max = currentMax;
    for (Map.Entry<K, V> entry : restartableMap.entrySet()) {
      final int current = entry.getValue().getObjectIndex();
      if (max < current) {
        max = current;
      }
      if (valueConsumer != null) {
        valueConsumer.accept(entry.getValue());
      }
    }
    return max;
  }

  /**
   * Removes all first level children and trigger removal of next level children.
   * <p>
   * Note: An iterator cannot be used here as {@link com.terracottatech.frs.object.RestartableMap} does
   * not support a remove from its iterator.
   *
   * @param parentId The parent that is being removed
   */
  private void removeAllChildConfiguration(K1 parentId) {
    List<String> removeList = new ArrayList<>();
    for (Map.Entry<String, V2> entry : childConfigMap.entrySet()) {
      if (entry.getValue().getParentId().equals(parentId)) {
        removeAllOtherChildConfiguration(entry.getKey());
        removeList.add(entry.getKey());
      }
    }
    for (String id : removeList) {
      childConfigMap.remove(id);
    }
  }

  private void removeAllOtherChildConfiguration(String parentId) {
    List<String> nextToRemove = new ArrayList<>();
    for (Map.Entry<Class<? extends ChildConfiguration<String>>, RestartableGenericMap<String,
        ? extends ChildConfiguration<String>>> mapOfMapEntry : otherChildMapOfMaps.entrySet()) {
      List<String> removeList = new ArrayList<>();
      for (Map.Entry<String, ? extends ChildConfiguration<String>> entry : mapOfMapEntry.getValue().entrySet()) {
        if (entry.getValue().getParentId().equals(parentId)) {
          nextToRemove.add(entry.getKey());
          removeList.add(entry.getKey());
        }
      }
      for (String r : removeList) {
        mapOfMapEntry.getValue().remove(r);
      }
    }
    for (String id : nextToRemove) {
      removeAllOtherChildConfiguration(id);
    }
  }

  public String getMetaStorePathAsString() {
    return restartLogLifecycle.getLogDir().getAbsolutePath();
  }

  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getMetaStore() {
    return restartLogLifecycle.getRestartStore();
  }
}
