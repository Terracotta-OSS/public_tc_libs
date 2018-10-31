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

package com.terracottatech.sovereign.impl.persistence.hybrid;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.persistence.AbstractBaseStorageTest;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by cschanck on 7/21/2015.
 */
public class SovereignHybridStorageTest extends AbstractBaseStorageTest  {

  private SovereignHybridStorage storage;
  private PersistenceRoot root;

  @Override
  public SovereignHybridStorage getOrCreateStorage(boolean reopen) throws IOException {
    if (this.storage == null) {
      this.root = new PersistenceRoot(file, reopen? PersistenceRoot.Mode.DONTCARE: PersistenceRoot.Mode.ONLY_IF_NEW);
      this.storage = new SovereignHybridStorage(root, SovereignBufferResource.unlimited());
    }
    return this.storage;
  }

  @Override
  public void shutdownStorage() throws IOException {
    if (this.storage != null) {
      this.storage.shutdown();
      this.storage = null;

      Runtime runtime = Runtime.getRuntime();
      for (int i = 0; i < 10; i++) {
        runtime.gc();
        runtime.runFinalization();
        try {
          TimeUnit.MILLISECONDS.sleep(100L);
        } catch (InterruptedException e) {
          throw new AssertionError("Interrupted in shutdownStorage while running GC");
        }
      }
    }
  }

}
