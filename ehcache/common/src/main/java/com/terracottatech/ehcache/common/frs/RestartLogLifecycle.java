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
package com.terracottatech.ehcache.common.frs;

import com.terracottatech.ehcache.common.frs.exceptions.CorruptRestartLogException;
import com.terracottatech.ehcache.common.frs.exceptions.RestartLogProcessingException;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.recovery.RecoveryException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Handle lifecycle per FRS log.
 *
 * @author RKAV
 */
public final class RestartLogLifecycle {
  private static final String SYNC_START = "Restart Log Synchronous Recovery";
  private static final String ASYNC_START = "Restart Log Asynchronous Recovery";
  private static final String ASYNC_START_WAIT = "Restart Log Wait For Recovery";
  private static final String STOP = "Restart Log Shutdown";

  private final File logDir;
  private final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore;

  public File getLogDir() {
    return logDir;
  }

  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getRestartStore() {
    return restartStore;
  }

  private boolean started;
  private Future<Void> startupFuture;

  public RestartLogLifecycle(File logDir, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    this.startupFuture = null;
    this.logDir = logDir;
    this.restartStore = restartStore;
    this.started = false;
  }

  public void startStore() {
    try {
      if (!started && startupFuture == null) {
        restartStore.startup().get();
        started = true;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RestartLogProcessingException(logDir, SYNC_START, e);
    } catch (RecoveryException re) {
      throw new CorruptRestartLogException(logDir, SYNC_START, re);
    }
  }

  public void shutStore() {
    try {
      if (startupFuture != null) {
        startupFuture.cancel(true);
        startupFuture = null;
      }
      // shutdown the cache data store area
      restartStore.shutdown();
    } catch (InterruptedException e) {
      throw new RestartLogProcessingException(logDir, STOP, e);
    }
  }

  public void startStoreAsync() {
    try {
      startupFuture = restartStore.startup();
    } catch (InterruptedException | RecoveryException e) {
      throw new RestartLogProcessingException(logDir, ASYNC_START, e);
    }
  }

  public void waitOnCompletion() {
    try {
      if (started || startupFuture == null) {
        return;
      }
      startupFuture.get();
      startupFuture = null;
      started = true;
    } catch (InterruptedException e) {
      throw new RestartLogProcessingException(logDir, ASYNC_START_WAIT, e);
    } catch (ExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof RecoveryException) {
        throw new CorruptRestartLogException(logDir, ASYNC_START_WAIT, e.getCause());
      } else {
        throw new RestartLogProcessingException(logDir, ASYNC_START_WAIT, e);
      }
    }
  }
}
