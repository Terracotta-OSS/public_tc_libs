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

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

public class LongListProxyTest {

  @Test
  public void testNothing() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    Assert.assertThat(LongListProxy.listSizeForPageSize(pgSize), is((256 - 2 - 8) / 8));
    Assert.assertThat(new LongListProxy().size(new Accessor(db)), is(0));
    Assert.assertThat(new LongListProxy().getRevision(new Accessor(db)), is(0L));
  }

  @Test
  public void testSizeAddressRevision() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();

    Assert.assertThat(proxy.size(new Accessor(db)), is(0));
    proxy.setSize(new Accessor(db), (short) 10);
    Assert.assertThat(proxy.size(new Accessor(db)), is(10));

    Assert.assertThat(proxy.getRevision(new Accessor(db)), is(0L));
    proxy.setRevision(new Accessor(db), 100l);
    Assert.assertThat(proxy.getRevision(new Accessor(db)), is(100l));
  }

  @Test
  public void testSetsGets() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);

    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    for (int i = 0; i < cap; i++) {
      Assert.assertThat(proxy.get(a, i), is((long) i));
    }
  }

  @Test
  public void testInsertsBeginning() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);

    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    proxy.insert(cap - 1, a, 0, 100);
    Assert.assertThat(proxy.get(a, 0), is(100L));
    for (int i = 1; i < cap; i++) {
      Assert.assertThat(proxy.get(a, i), is((long) (i - 1)));
    }
  }

  @Test
  public void testInsertsEnd() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);

    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    proxy.insert(cap - 1, a, cap - 1, 100);
    Assert.assertThat(proxy.get(a, cap - 1), is(100L));
    for (int i = 1; i < (cap - 1); i++) {
      Assert.assertThat(proxy.get(a, i), is((long) (i)));
    }
  }

  @Test
  public void testInsertsMiddle() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);

    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    int point = cap / 2;
    proxy.insert(cap - 1, a, point, 100);
    Assert.assertThat(proxy.get(a, point), is(100L));
    for (int i = 0; i < cap; i++) {
      if (i != point) {
        Assert.assertThat(proxy.get(a, i), is((long) (i + ((i > point) ? -1 : 0))));
      }
    }
  }

  @Test
  public void testDeleteBeginning() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);
    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    proxy.delete(cap, a, 0);
    for (int i = 0; i < cap - 1; i++) {
      Assert.assertThat(proxy.get(a, i), is((long) (i + 1)));
    }
  }

  @Test
  public void testDeleteEnd() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);
    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    proxy.delete(cap, a, cap - 1);
    for (int i = 0; i < cap - 1; i++) {
      Assert.assertThat(proxy.get(a, i), is((long) (i)));
    }
  }

  @Test
  public void testDeleteMiddle() {
    final int pgSize = 256;
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(pgSize));
    NIOBufferUtils.fill(db.delegate(), (byte) 0);
    LongListProxy proxy = new LongListProxy();
    int cap = LongListProxy.listSizeForPageSize(pgSize);
    Accessor a = new Accessor(db);
    for (int i = 0; i < cap; i++) {
      proxy.set(a, i, i);
    }
    int point = cap / 2;
    proxy.delete(cap, a, point);
    for (int i = 0; i < cap - 1; i++) {
      Assert.assertThat(proxy.get(a, i), is((long) (i + ((i >= point) ? 1 : 0))));
    }
  }
}
