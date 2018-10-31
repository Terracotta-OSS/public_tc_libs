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

import org.junit.Assert;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class PageSourceLocationTest {

  @Test
  public void testCreationUpfront() throws IOException {
    PageSourceLocation psl = new PageSourceLocation(
      new UpfrontAllocatingPageSource(new HeapBufferSource(), 512 * 1024 * 1024l, 512 * 1024), 512 * 1024 * 1024l,
      512 * 1024);
    psl.destroy();
    psl.freeAll();
  }

  @Test
  public void testLegalAllocationUpfront() throws IOException {
    PageSourceLocation psl = new PageSourceLocation(
      new UpfrontAllocatingPageSource(new HeapBufferSource(), 512 * 1024l, 512), 512 * 1024l, 512);
    ByteBuffer b = psl.allocateBuffer(512);
    Assert.assertThat(b, notNullValue());
    psl.free(b);
    psl.freeAll();
  }

  @Test
  public void testIllegalAllocationUpfront() throws IOException {
    UpfrontAllocatingPageSource pgsrc = new UpfrontAllocatingPageSource(new HeapBufferSource(), 512 * 1024l, 512);
    PageSourceLocation psl = new PageSourceLocation(pgsrc, 512 * 1024l, 512);
    ByteBuffer b = psl.allocateBuffer(513);
    Assert.assertThat(b, nullValue());
    psl.freeAll();
  }

  @Test
  public void testAllocationFree() {
    UpfrontAllocatingPageSource pgsrc = new UpfrontAllocatingPageSource(new HeapBufferSource(), 512 * 1024l,
      512 * 1024);
    PageSourceLocation psl = new PageSourceLocation(pgsrc, 512 * 1024l, 512 * 1024);
    ByteBuffer b;
    b = psl.allocateBuffer(512 * 1024);
    Assert.assertThat(b, notNullValue());

    ByteBuffer b1 = psl.allocateBuffer(1024);
    Assert.assertThat(b1, nullValue());

    psl.free(b);
    b = psl.allocateBuffer(512 * 1024);
    Assert.assertThat(b, notNullValue());

    psl.freeAll();
    b = psl.allocateBuffer(512 * 1024);
    Assert.assertThat(b, notNullValue());

    psl.freeAll();
  }

  @Test
  public void testCreationUnlimited() throws IOException {
    PageSourceLocation psl = new PageSourceLocation(new UnlimitedPageSource(new HeapBufferSource()), Long.MAX_VALUE,
      Integer.MAX_VALUE);
    psl.freeAll();
  }

  @Test
  public void testLegalAllocationUnlimited() throws IOException {
    PageSourceLocation psl = new PageSourceLocation(new UnlimitedPageSource(new HeapBufferSource()), Long.MAX_VALUE,
      Integer.MAX_VALUE);
    ByteBuffer b = psl.allocateBuffer(512);
    Assert.assertThat(b, notNullValue());
    psl.free(b);
    psl.freeAll();
  }

  @Test
  public void testIllegalAllocationUnlimited() throws IOException {
    PageSourceLocation psl = new PageSourceLocation(new UnlimitedPageSource(new HeapBufferSource()), Long.MAX_VALUE,
      512);
    ByteBuffer b = psl.allocateBuffer(513);
    Assert.assertThat(b, nullValue());
    psl.freeAll();
  }

}
