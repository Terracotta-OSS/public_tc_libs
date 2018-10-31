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

package com.terracottatech.sovereign.impl.persistence.support;

import com.terracottatech.sovereign.common.utils.FileUtils;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 6/30/2015.
 */
public class PersistenceRootTest {

  private File prepFile(boolean createDir, boolean createMeta, boolean createData) throws IOException {
    File f = File.createTempFile("testes", "testes");
    f.delete();
    if (createDir) {
      f.mkdirs();
      if (createMeta) {
        File m = new File(f, PersistenceRoot.META);
        m.mkdirs();
        FileUtils.touch(new File(m, "foo"));
      }
      if (createData) {
        File m = new File(f, PersistenceRoot.DATA);
        m.mkdirs();
        FileUtils.touch(new File(m, "bar"));
      }
    }
    return f;
  }

  @Test
  public void testEmptyOpenNew() throws IOException {
    File f = prepFile(false, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.asNew(f);
      assertThat(root.isReopen(), is(false));
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testEmptyDirOpenNew() throws IOException {
    File f = prepFile(true, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.asNew(f);
      assertThat(root.isReopen(), is(false));
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testOpenNewFails() throws IOException {
    File f = prepFile(true, true, false);
    try {
      PersistenceRoot root = PersistenceRoot.asNew(f);
      Assert.fail();
    } catch (IOException e) {
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testReOpenFails() throws IOException {
    File f = prepFile(false, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.reopen(f);
      Assert.fail();
    } catch (IOException e) {
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testReOpenEmptyDirFails() throws IOException {
    File f = prepFile(true, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.reopen(f);
      Assert.fail();
    } catch (IOException e) {
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testReOpenMetaOnly() throws IOException {
    File f = prepFile(true, true, false);
    try {
      PersistenceRoot root = PersistenceRoot.reopen(f);
      assertThat(root.isReopen(), is(true));
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testReOpenDataOnlyFails() throws IOException {
    File f = prepFile(true, false, true);
    try {
      PersistenceRoot root = PersistenceRoot.reopen(f);
      Assert.fail();
    } catch (IOException e) {
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testReOpenSuccess() throws IOException {
    File f = prepFile(true, true, true);
    try {
      PersistenceRoot root = PersistenceRoot.reopen(f);
      assertThat(root.isReopen(), is(true));
    } catch (IOException e) {
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testDontCareEmpty() throws IOException {
    File f = prepFile(false, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.open(f);
      assertThat(root.isReopen(), is(false));
    } catch (IOException e) {
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testDontCareFull() throws IOException {
    File f = prepFile(true, true, true);
    try {
      PersistenceRoot root = PersistenceRoot.open(f);
      assertThat(root.isReopen(), is(true));
    } catch (IOException e) {
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testDontCareMetaOnly() throws IOException {
    File f = prepFile(true, true, false);
    try {
      PersistenceRoot root = PersistenceRoot.open(f);
      assertThat(root.isReopen(), is(true));
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testDontCareMetaAndData() throws IOException {
    File f = prepFile(true, true, true);
    try {
      PersistenceRoot root = PersistenceRoot.open(f);
      assertThat(root.isReopen(), is(true));
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testCreateEmpty() throws IOException {
    File f = prepFile(false, false, false);
    try {
      PersistenceRoot root = PersistenceRoot.create(f);
      assertThat(root.isReopen(), is(false));
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }

  @Test
  public void testCreateOvertop() throws IOException {
    File f = prepFile(true, true, true);
    try {
      PersistenceRoot root = PersistenceRoot.create(f);
      assertThat(root.isReopen(), is(false));
      assertThat(root.getMetaRoot().listFiles().length,is(0));
      assertThat(root.getDataRoot().listFiles().length,is(0));
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      FileUtils.deleteRecursively(f);
    }
  }
}
