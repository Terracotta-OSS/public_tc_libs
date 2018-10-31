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

package com.terracottatech.sovereign.btrees.stores.location;

import com.terracottatech.sovereign.common.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class DirectoryStoreLocationTest {

  private File dir;

  @Before
  public void before() throws IOException {
    File f = File.createTempFile("play", "space");
    f.delete();
    f.mkdirs();
    this.dir = f;
  }

  @After
  public void after() throws IOException {
    if (this.dir != null) {
      FileUtils.deleteRecursively(this.dir);
    }
  }

  @Test
  public void testShouldNotExist() throws IOException {
    DirectoryStoreLocation dsl = new DirectoryStoreLocation(new File(dir, "dsl"));
    Assert.assertThat(dsl.exists(),is(false));
  }

  @Test
  public void testCreation() throws IOException {
    DirectoryStoreLocation dsl= new DirectoryStoreLocation(new File(dir, "dsl"));
    dsl.ensure();
    Assert.assertThat(dsl.exists(),is(true));
  }

  @Test
  public void testDoubleCreation() throws IOException {
    DirectoryStoreLocation dsl1= new DirectoryStoreLocation(new File(dir, "dsl"));
    dsl1.ensure();
    Assert.assertThat(dsl1.exists(),is(true));
    DirectoryStoreLocation dsl2= new DirectoryStoreLocation(new File(dir, "dsl"));
    Assert.assertThat(dsl1.exists(),is(true));
  }

  @Test
  public void testCreationDeletion() throws IOException {
    DirectoryStoreLocation dsl1= new DirectoryStoreLocation(new File(dir, "dsl"));
    dsl1.ensure();
    Assert.assertThat(dsl1.exists(),is(true));
    dsl1.destroy();
    DirectoryStoreLocation dsl2= new DirectoryStoreLocation(new File(dir, "dsl"));
    Assert.assertThat(dsl1.exists(),is(false));
  }

}
