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
package com.terracottatech.ehcache.clustered.server.services.frs.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.frs.RestartStore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.terracotta.management.service.monitoring.registry.provider.AliasBinding;

import java.io.File;
import java.nio.ByteBuffer;

@CommonComponent
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class RestartStoreBinding extends AliasBinding {

  private final FrsDataLogIdentifier frsDataLogIdentifier;
  private final File storageLocation;

  public RestartStoreBinding(FrsDataLogIdentifier frsDataLogIdentifier, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore, File storageLocation) {
    super(getRestartableStoreId(frsDataLogIdentifier), restartStore);
    this.frsDataLogIdentifier = frsDataLogIdentifier;
    this.storageLocation = storageLocation;
  }

  FrsDataLogIdentifier getFrsDataLogIdentifier() {
    return frsDataLogIdentifier;
  }

  public String getRestartableStoreId() {
    return getRestartableStoreId(frsDataLogIdentifier);
  }

  public String getRestartableStoreRoot() {
    return frsDataLogIdentifier.getRootPathName();
  }

  public String getRestartableStoreContainer() {
    return frsDataLogIdentifier.getContainerName();
  }

  public String getRestartableLogName() {
    return frsDataLogIdentifier.getDataLogName();
  }

  public String getRestartableStorageLocation() {
    return storageLocation.getAbsolutePath();
  }

  @Override
  @SuppressWarnings("unchecked")
  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getValue() {
    return (RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>) super.getValue();
  }

  private static String getRestartableStoreId(FrsDataLogIdentifier identifier) {
    return String.join("#",
        identifier.getRootPathName(),
        identifier.getContainerName(),
        identifier.getDataLogName());
  }

}