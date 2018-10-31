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

package com.terracottatech.sovereign.btrees.utils;

import com.terracottatech.sovereign.common.utils.FileUtils;
import com.terracottatech.sovereign.common.utils.FileVersioner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;

/**
 * @author cschanck
 */
public class FileVersionerTest {
  private File dir;

  @Before
  public void before() throws IOException {
    dir = File.createTempFile("lob", "lob");
    dir.delete();
  }

  @After
  public void after() throws IOException {
    if (dir.exists()) {
      FileUtils.deleteRecursively(dir);
    }
  }

  @Test
  public void testOpenIsEmpty() throws IOException {
    FileVersioner versioner = new FileVersioner(dir, "fun.v");
    Assert.assertThat(versioner.getLatestExistingFileIndex(),is(-1));
    Assert.assertThat(versioner.getAllVersionedFiles().length,is(0));
  }

  @Test
  public void testOpenAdd() throws IOException {
    FileVersioner versioner = new FileVersioner(dir, "fun.v");
    File f=versioner.getNextVersionedFile();
    Assert.assertThat(f.getName(),is("fun.v0"));
    FileUtils.touch(f);
    Assert.assertThat(versioner.getLatestExistingFileIndex(),is(0));
    Assert.assertThat(versioner.getAllVersionedFiles().length,is(1));
    f=versioner.getNextVersionedFile();
    FileUtils.touch(f);
    Assert.assertThat(versioner.getLatestExistingFileIndex(),is(1));
    Assert.assertThat(versioner.getAllVersionedFiles().length,is(2));
  }

  @Test
  public void testOpenAddWithHole() throws IOException {
    FileVersioner versioner = new FileVersioner(dir, "fun.v");
    File f1=versioner.getNextVersionedFile();
    FileUtils.touch(f1);

    File f2 = versioner.getNextVersionedFile();
    FileUtils.touch(f2);

    f1.delete();

    Assert.assertThat(versioner.getLatestExistingFileIndex(),is(1));
    Assert.assertThat(versioner.getAllVersionedFiles().length,is(1));
  }
}
