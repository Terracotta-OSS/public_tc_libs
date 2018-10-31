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
package com.terracottatech.sovereign.impl.persistence;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.configuration.PersistentStorageType;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPersistentStorage extends AbstractStorage {
  protected volatile boolean wasShutdown = false;
  private final AtomicBoolean dirty = new AtomicBoolean(true);

  private SharedSyncher syncher = new SharedSyncher(() -> {
    try {
      explicitFSynch();
    } catch (Throwable t) {
      if (!wasShutdown) {
        throw t;
      }
    }
  });

  public AbstractPersistentStorage(SovereignBufferResource resource) {
    super(resource);
  }

  @Override
  public  <Z extends TimeReference<Z>> void registerNewDataset(SovereignDatasetImpl<?> ds) {
    super.registerNewDataset(ds);
  }

  @Override
  protected void removeDataset(SovereignDatasetImpl<?> ds) {
    super.removeDataset(ds);
  }

  public SharedSyncher getSyncher() {
    return syncher;
  }

  protected void killSyncher() {
    syncher.stop();
  }

  public void markDirty() {
    dirty.set(true);
  }

  /**
   * Sync changes to disk.
   */
  public void fsynch(boolean force) {
    if (force) {
      dirty.set(false);
      explicitFSynch();
    } else if (dirty.compareAndSet(true, false)) {
      explicitFSynch();
    }
  }

  protected abstract void explicitFSynch();

  public abstract SovereignDatasetDiskDurability getDefaultDiskDurability();
  public abstract boolean isCompatible(PersistentStorageType storageType);
  public abstract PersistentStorageType getPersistentStorageType();
  public abstract PersistenceRoot getPersistenceRoot();
}
