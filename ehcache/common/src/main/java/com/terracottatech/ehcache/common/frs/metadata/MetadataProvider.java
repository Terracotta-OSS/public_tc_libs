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
package com.terracottatech.ehcache.common.frs.metadata;

import com.tc.classloader.CommonComponent;
import com.terracottatech.frs.RestartStore;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * Generic Interface to handle one or more metadata log containing multi-level hierarchical config metadata stored in FRS.
 * <p>
 * A minimal Metadata system will have atleast the root configuration and a first level child configuration. The first level
 * child then points to data logs through data log identifiers, apart from having other configuration data. In addition,
 * this metadata system allows any additional level of children of different configuration types as extensions.
 * See {@link MetadataProviderBuilder} to understand how to add additional child types.
 * <p>
 * This interface hides the distribution of meta-data across multiple FRS containers and provides a single interface
 * to the outside world.
 * <p>
 * A container of FRS can contain the following:
 *    a metadata log
 *    one or more data logs; the file location of these data logs can be derived from the config objects contained
 *    in the metadata
 *
 * ..and there could be multiple such FRS containers.
 * <p>
 * For e.g. an example of multi-level config data within the metadata for <i>ehcache</i> could be as follows:
 *      cachemanager configuration (root) --> Cache Configuration (first level child)
 *                 --> State Repository configuration (other child)
 * It is up to the user to design and decide on the configuration hierarchy.
 * <p>
 * Note: Implemented at language level 6, as clients of this library could include <em>standalone ehcache</em>.
 *
 * @param <K1> Key type of parent type
 * @param <V1> Value type of parent
 * @param <V2> Value type of first level child; note that the key type within metadata system for all children types
 *            are assumed to be string, due to the need for composition and concatenation.
 *
 * @author RKAV
 */
@CommonComponent
public interface MetadataProvider<K1, V1 extends RootConfiguration, V2 extends ChildConfiguration<K1>> {
  /**
   * Bootstraps all containers that has existing metadata in it. This method returns only after the bootstrap
   * is complete.
   * <p>
   * Note that the underlying implementation handling multiple containers may choose to bootstrap these containers
   * in parallel. This is completely legal as long as this method returns after all containers are bootstrapped.
   *
   * @return a list of already existing data logs
   */
  Set<FrsDataLogIdentifier> bootstrapAllContainers();

  /**
   * Shuts down all the containers. After this metadata will no longer be available for any container.
   */
  void shutdownAllContainers();

  /**
   * Gets all root configuration spread across all containers.
   *
   * @return a map of root configuration objects
   */
  Map<K1, V1> getAllRootConfiguration();

  /**
   * Gets the first level children from all containers.
   *
   * @param rootId Id of the root object
   * @return a map of first level children spread across all containers
   */
  Map<String, V2> getFirstLevelChildConfigurationByRoot(K1 rootId);

  /**
   * Gets the child configuration by a given data log from a single container
   *
   * @param dataLogId Data log identifier
   * @return a map of first level children from a single container pointing to the specified data log.
   */
  Map<String, V2> getFirstLevelChildConfigurationByDataLog(FrsDataLogIdentifier dataLogId);

  /**
   * Gets the nth level children by a given parent, where n is > 1.
   *
   * @param parentId parent identifier
   * @param childType Type of child
   * @param <V3> Type of child configuration
   * @return map of nth level children from container having the given higher level parent.
   */
  <V3 extends ChildConfiguration<String>> Map<String, V3> getNthLevelOtherChildConfigurationByParent(String parentId,
                                                                                                     Class<V3> childType);

  /**
   * Gets the nth level children who are pointing to the same data log from a container that has the data log.
   *
   * @param dataLogId data log identifier
   * @param childType configuration type
   * @param <V3> configuration type
   * @return map of nth level children of the given config type
   */
  <V3 extends ChildConfiguration<String>> Map<String, V3> getNthLevelOtherChildConfigurationByDataLog(FrsDataLogIdentifier dataLogId,
                                                                                                      Class<V3> childType);

  /**
   * Get a specific child configuration entry, given entry ID.
   *
   * @param containerId Id of the container where we need to look for the entry
   * @param composedId unique id of a particular first level child
   * @return entry, if found.. null, otherwise
   */
  V2 getChildConfigEntry(FrsContainerIdentifier containerId, String composedId);

  /**
   * Get the  extended child configuration entry, based on the entry id.
   *
   * @param containerId Id of the container where we need to look for the entry
   * @param composedId unique id of the nth level child
   * @return entry, if found.. null, otherwise
   */
  <V3 extends ChildConfiguration<String>> V3 getNthLevelOtherChildConfigEntry(
      FrsContainerIdentifier containerId, String composedId, Class<V3> otherChildType);

  /**
   * Adds a root configuration object.
   *
   * @param rootId Root configuration object identifier
   * @param parentConfiguration Configuration of the new root object
   * @return true, if added to any..false, if it was existing in all of the containers
   */
  boolean addRootConfiguration(K1 rootId, V1 parentConfiguration);

  /**
   * Adds a first level child against the root. The child configuration object passed has sufficient information
   * denoting which container this child object needs to be added.
   *
   * @param composedId Unique Id that contains parent Id in some form to ensure ids do not repeat and is unique
   * @param childConfiguration Child configuration object
   * @return true, if added to any of containers, ..false, if it was existing in all
   */
  boolean addFirstLevelChildConfiguration(String composedId, V2 childConfiguration);

  /**
   * Adds a nth level child.The child configuration object has sufficient information denoting which container the
   * parent exists and where this child should be added.
   *
   * @param composedId Unique id that ensure ids does not repeat across siblings
   * @param childConfiguration Configuration object
   * @param childType configuration class
   * @param <V3> Type of configuration
   * @return true, if added..false, if it was already existing
   */
  <V3 extends ChildConfiguration<String>> boolean addNthLevelOtherChildConfiguration(String composedId,
                                                                                     V3 childConfiguration,
                                                                                     Class<V3> childType);

  /**
   * Removes a root and its entire configuration tree from a given container.
   *
   * @param frsId container identifier
   * @param rootId root identifier
   */
  void removeRootConfigurationFrom(FrsContainerIdentifier frsId, K1 rootId);

  /**
   * Removes a first level child and its entire sub tree from a given container.
   *
   * @param frsId container identifier
   * @param childConfigurationId child configuration identifier
   */
  void removeChildConfigurationFrom(FrsContainerIdentifier frsId, String childConfigurationId);

  /**
   * Removes nth level child and its entire sub tree from a given container.
   *
   * @param frsId Container identifier
   * @param childConfigurationId child configuration identifier
   * @param childType child configuration class
   * @param <V3> type of the configuration
   */
  <V3 extends ChildConfiguration<String>> void removeNthLevelOtherChildConfigurationFrom(FrsContainerIdentifier frsId,
                                                                                         String childConfigurationId,
                                                                                         Class<V3> childType);

  /**
   * Return a map of all restart stores used by {@code MetadataProvider} and their file location.
   *
   * @return a map containing all metadata.
   */
  Map<String, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>> getMetaStores();
}