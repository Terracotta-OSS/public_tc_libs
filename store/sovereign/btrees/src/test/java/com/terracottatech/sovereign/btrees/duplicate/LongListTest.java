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

package com.terracottatech.sovereign.btrees.duplicate;

import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

public class LongListTest {

  @Test
  public void testNothing() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    Assert.assertThat(ll.size(), is(0));
    Assert.assertThat(ll.getRevision(), is(-1L));
    Assert.assertThat(ll.isDirty(), is(false));
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.size(), is(0));
    Assert.assertThat(ll2.getRevision(), is(-1L));
    Assert.assertThat(ll2.isDirty(), is(false));
  }

  @Test
  public void testAddressRevision() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    ll.setRevision(1000);
    Assert.assertThat(ll.getRevision(), is(1000L));
    Assert.assertThat(ll.isDirty(), is(true));
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.getRevision(), is(1000L));
    Assert.assertThat(ll2.isDirty(), is(false));
  }

  @Test
  public void testFillSmall() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    for (int i = 0; i < 5; i++) {
      ll.add(i);
    }
    Assert.assertThat(ll.size(), is(5));
    Assert.assertThat(ll.isDirty(), is(true));
    for (int i = 0; i < 5; i++) {
      Assert.assertThat(ll.get(i), is((long) i));
    }
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.size(), is(5));
    for (int i = 0; i < 5; i++) {
      Assert.assertThat(ll2.get(i), is((long) i));
    }
    Assert.assertThat(ll2.isDirty(), is(false));
  }

  @Test
  public void testFillSpill() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    for (int i = 0; i < 200; i++) {
      ll.add(i);
    }
    Assert.assertThat(ll.size(), is(200));
    for (int i = 0; i < 200; i++) {
      Assert.assertThat(ll.get(i), is((long) i));
    }
    Assert.assertThat(ll.isDirty(), is(true));
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.size(), is(200));
    for (int i = 0; i < 200; i++) {
      Assert.assertThat(ll2.get(i), is((long) i));
    }
    Assert.assertThat(ll2.isDirty(), is(false));
  }

  @Test
  public void testFillSmallDelete() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    for (int i = 0; i < 10; i++) {
      ll.add(i);
    }
    for (int i = 0; i < 4; i++) {
      ll.remove(3 + i);
    }
    Assert.assertThat(ll.size(), is(6));
    for (int i = 0; i < ll.size(); i++) {
      Assert.assertThat(ll.get(i), is((long) (i + ((i >= 3) ? 4 : 0))));
    }
    Assert.assertThat(ll.isDirty(), is(true));
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.size(), is(6));
    for (int i = 0; i < ll2.size(); i++) {
      Assert.assertThat(ll2.get(i), is((long) (i + ((i >= 3) ? 4 : 0))));
    }
    Assert.assertThat(ll2.isDirty(), is(false));
  }

  @Test
  public void testFillSpillDelete() {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), 256, 1024 * 1024, 1024 * 1024 * 1024);
    LongList ll = new LongList(256);
    for (int i = 0; i < 200; i++) {
      ll.add(i);
    }
    for (int i = 0; i < 25; i++) {
      ll.remove(100 + i);
    }
    Assert.assertThat(ll.size(), is(175));
    for (int i = 0; i < ll.size(); i++) {
      Assert.assertThat(ll.get(i), is((long) (i + ((i >= 100) ? 25 : 0))));
    }
    Assert.assertThat(ll.isDirty(), is(true));
    ByteBuffer tmp = NIOBufferUtils.dup(ll.marshalForWrite());
    LongList ll2 = new LongList(tmp, 256);
    Assert.assertThat(ll2.size(), is(175));
    for (int i = 0; i < ll2.size(); i++) {
      Assert.assertThat(ll2.get(i), is((long) (i + ((i >= 100) ? 25 : 0))));
    }
    Assert.assertThat(ll2.isDirty(), is(false));
  }

}
