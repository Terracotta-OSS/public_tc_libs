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

/**
 * Contains the core data storage components of Terracotta Store.
 * <p>
 *   Terracotta Store stores data in {@link com.terracottatech.store.Dataset}s.
 *   Datasets are composed of {@link com.terracottatech.store.Record}s that contain a unique key (within the dataset),
 *   and a set of {@link com.terracottatech.store.Cell Cell}s.
 * <p>
 *   Datasets can be constructed through the {@link com.terracottatech.store.manager.DatasetManager#embedded()} and
 *   {@link com.terracottatech.store.manager.DatasetManager#clustered(java.net.URI)} static methods.  These methods
 *   return builders which can be used to construct a {@code DatasetManager} instance.
 */
package com.terracottatech.store;
