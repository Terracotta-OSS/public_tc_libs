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
package com.terracottatech.ehcache.clustered.server;

import com.terracottatech.config.data_roots.DataDirectoriesConfigImpl;
import com.terracottatech.data.config.DataDirectories;
import com.terracottatech.data.config.DataRootMapping;
import org.junit.rules.TemporaryFolder;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;

import java.io.IOException;
import java.math.BigInteger;

public class PassthroughTestHelper {

  private static final DataDirectories directories = new DataDirectories();

  public static OffheapResourcesType getOffheapResources(int size, MemoryUnit unit, String... varargs) {
    OffheapResourcesType resources = new OffheapResourcesType();
    for (String vararg : varargs) {
      resources.getResource().add(getResource(vararg, size, unit));
    }
    return resources;
  }

  public static void clearDataDirectories() {
    directories.getDirectory().clear();
  }

  public static DataDirectories getDataDirectories() {
    return directories;
  }

  public static DataDirectoriesConfigImpl addDataRoots(String baseRootName, TemporaryFolder folder, int count) throws IOException {
    for (int i = 1; i <= count; i++) {
      DataRootMapping dataRootMapping = new DataRootMapping();
      dataRootMapping.setName(baseRootName+i);
      dataRootMapping.setValue(folder.newFolder().getAbsolutePath());
      directories.getDirectory().add(dataRootMapping);
    }

    return new DataDirectoriesConfigImpl(null, directories);
  }
  public static OffheapResourcesType getOffheapResourcesType(String resourceName, int size, MemoryUnit unit) {
    OffheapResourcesType resources = new OffheapResourcesType();
    resources.getResource().add(getResource(resourceName, size, unit));
    return resources;
  }

  public static ResourceType getResource(String resourceName, int size, MemoryUnit unit) {
    final ResourceType resource = new ResourceType();
    resource.setName(resourceName);
    resource.setUnit(unit);
    resource.setValue(BigInteger.valueOf((long)size));
    return resource;
  }
}
