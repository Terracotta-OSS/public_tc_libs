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

import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;

import java.io.IOException;

public class ErrorStorageFactory implements InternalStorageFactory {
  private final String message;
  private final Throwable error;

  public ErrorStorageFactory(String message) {
    this(message, null);
  }

  public ErrorStorageFactory(String message, Throwable error) {
    this.message = message;
    this.error = error;
  }

  @Override
  public SovereignStorage<?, ?> getStorage(StorageConfiguration storageConfiguration) throws StorageFactoryException {
    if (error == null) {
      throw new StorageFactoryException(message);
    } else if (message == null) {
      throw new StorageFactoryException(error.getMessage(), error);
    } else {
      throw new StorageFactoryException(message + ": " + error.getMessage(), error);
    }
  }

  @Override
  public void shutdownStorage(StorageConfiguration storageConfiguration, SovereignStorage<?, ?> storage) throws IOException {
  }

  @Override
  public void close() {
  }

  @Override
  public void prepareForSynchronization() {
  }

  @Override
  public BackupCapable getBackupCoordinator() {
    return null;
  }
}