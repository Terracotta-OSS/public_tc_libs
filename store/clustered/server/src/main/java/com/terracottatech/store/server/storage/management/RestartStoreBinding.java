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
package com.terracottatech.store.server.storage.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.store.server.storage.factory.PersistentStorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.terracotta.management.service.monitoring.registry.provider.AliasBinding;

import java.io.File;
import java.nio.ByteBuffer;

@CommonComponent
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class RestartStoreBinding extends AliasBinding {

  private final File storageLocation;
  private final String containerName;
  private final String dataLogName;
  private final String diskResource;

  public RestartStoreBinding(RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore, File storageLocation, String diskResource) {
    super(retrieveRestartableStoreId(diskResource), restartStore);
    this.diskResource = diskResource;
    this.containerName = PersistentStorageFactory.STORE_DIRECTORY;
    this.dataLogName = PersistenceRoot.DATA;
    this.storageLocation = new File(storageLocation, dataLogName);
  }

  public String getRestartableStoreId() {
    return retrieveRestartableStoreId(diskResource);
  }

  static String retrieveRestartableStoreId(String diskResource) {
    return String.join("#",
        diskResource,
        PersistentStorageFactory.STORE_DIRECTORY,
        PersistenceRoot.DATA);
  }

  public File getStorageLocation() {
    return storageLocation;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getValue() {
    return (RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>) super.getValue();
  }

  public String getContainerName() {
    return containerName;
  }

  public String getDataLogName() {
    return dataLogName;
  }

  public String getDiskResource() {
    return diskResource;
  }
}
