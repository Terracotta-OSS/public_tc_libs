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
package org.terracotta.catalog;

import org.terracotta.connection.entity.Entity;

import java.util.Map;

/**
 *
 */
public interface SystemCatalog extends Entity {
  long VERSION = 1L;
  String ENTITY_NAME = "staticCataloger";

  boolean makeLeader();
  boolean releaseLeader();
  boolean isLeader();

  byte[] getConfiguration(Class<?> type, String name);
  byte[] storeConfiguration(Class<?> type, String name, byte[] config);
  byte[] removeConfiguration(Class<?> type, String name);

  /**
   * @return key: name, value: type name
   */
  Map<String, String> listAll();

  /**
   * @param type filter out entities of a different type
   * @return key: name, value: config as bytes
   */
  Map<String, byte[]> listByType(Class<?> type);

  void referenceConfiguration(Class<?> type, String name);
  void releaseConfiguration(Class<?> type, String name);

  boolean tryLock(Class<?> type, String name);
  boolean unlock(Class<?> type, String name);

}
