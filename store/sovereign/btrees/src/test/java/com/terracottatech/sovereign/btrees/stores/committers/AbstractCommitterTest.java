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

package com.terracottatech.sovereign.btrees.stores.committers;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;

public abstract class AbstractCommitterTest {

  protected File file;

  @Before
  public void before() throws IOException {
    file = File.createTempFile("commit", "commit");
    file.delete();
  }

  @After
  public void after() throws IOException {
    if (file.exists()) {
      file.delete();
    }
  }

  public abstract Committer getCommitter() throws IOException;

  public abstract void closeCommitter() throws IOException;

  @Test
  public void testCreate() throws IOException {
    Committer commiter = getCommitter();
    ByteBuffer b = ByteBuffer.allocate(10);
    try {
      commiter.getCommitData(b);
      Assert.fail();
    } catch (IOException e) {
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    Committer commiter = getCommitter();
    for (int i = 1; i < 10; i++) {
      ByteBuffer b = ByteBuffer.allocate(20);
      while (b.hasRemaining()) {
        b.put((byte) i);
      }
      b.clear();
      commiter.commit(b);

      ByteBuffer b2 = ByteBuffer.allocate(20);
      commiter.getCommitData(b2);
      b2.clear();
      while (b2.hasRemaining()) {
        Assert.assertThat(b2.get(), is((byte) i));
      }
    }
  }

  @Test
  public void testReadWriteAcrossOpen() throws IOException {
    Committer commiter = getCommitter();
    if(commiter.isDurable()) {
      ByteBuffer b = ByteBuffer.allocate(20);
      while (b.hasRemaining()) {
        b.put((byte) 10);
      }
      b.clear();
      commiter.commit(b);
      closeCommitter();
      Committer commiter2 = getCommitter();
      ByteBuffer b2 = ByteBuffer.allocate(20);
      commiter2.getCommitData(b2);
      while (b2.hasRemaining()) {
        Assert.assertThat(b2.get(), is((byte) 10));
      }
    }
  }
}
