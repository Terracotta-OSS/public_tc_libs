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
package com.terracottatech.store.server.storage.factory;

import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.store.server.storage.management.DatasetStorageManagement;
import com.terracottatech.store.server.storage.offheap.BufferResourceFactory;

public class FRSStorageFactory extends RestartabilityStorageFactory {
  public FRSStorageFactory(BufferResourceFactory bufferResourceFactory, DataDirectories dataRootConfig,
                           DatasetStorageManagement datasetStorageManagement,
                           BackupCoordinatorFactory backupCoordinatorFactory) {
    super(bufferResourceFactory, dataRootConfig, datasetStorageManagement, backupCoordinatorFactory);
  }

  @Override
  protected AbstractPersistentStorage createStorage(PersistenceRoot persistenceRoot,
                                                    SovereignBufferResource bufferResource) {
    return new SovereignFRSStorage(persistenceRoot, bufferResource);
  }
}
