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

package com.terracottatech.store.embedded.persistence.demo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

/**
 * Runs the sample code in {@link EmbeddedPersistenceDemoDriver} as unit tests.
 */
public class EmbeddedPersistenceDemoDriver {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private Path dataPath;

  @Before
  public void setupDataPath() throws Exception {
    dataPath = tempFolder.newFolder().toPath();
  }

  @Test
  public void testDiskDemo() throws Exception {
    new EmbeddedPersistenceDemo().runPersistentDatasetCreateRetrieve(dataPath);
  }

  @Test
  public void testHybridDemo() throws Exception {
    new EmbeddedPersistenceDemo().runPersistentHybridDatasetCreateRetrieve(dataPath);
  }
}
