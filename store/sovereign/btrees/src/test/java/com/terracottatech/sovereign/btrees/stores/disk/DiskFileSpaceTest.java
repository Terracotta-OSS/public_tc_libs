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

package com.terracottatech.sovereign.btrees.stores.disk;

import com.terracottatech.sovereign.btrees.stores.AbstractSimpleStoreTestBase;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.location.DirectoryStoreLocation;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public class DiskFileSpaceTest extends AbstractSimpleStoreTestBase {
  protected File file;
  private DiskFileSpace afs;

  @Before
  public void before() throws IOException {
    file = File.createTempFile("lob", "lob");
    file.delete();
  }

  @After
  public void after() throws IOException {
    if (afs != null) {
      afs.close();
    }
    if (file.exists()) {
      FileUtils.deleteRecursively(file);
    }
  }

  @Override
  public SimpleStore getSimpleStoreReaderWriter() throws IOException {
    if (afs == null) {
      afs = new DiskFileSpace(new DirectoryStoreLocation(file), PageSourceLocation.offheap(),
        new SimpleBlockBuffer.Factory(PageSourceLocation.offheap(), new DiskBufferProvider.Unmapped(), 32 * 1024),
        128 * 1024 * 1024, 128 * 1024 * 1024, 4096);
    }
    return afs;
  }
}
