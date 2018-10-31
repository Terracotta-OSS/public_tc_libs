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

package com.terracottatech.sovereign.common.utils;

import com.terracottatech.sovereign.impl.utils.BitSetLongAddressListChunk;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * Created by cschanck on 5/6/2015.
 */
public class BitSetLongAddressListChunkTest {

  @Test
  public void testCreateFails() {

    try {
      new BitSetLongAddressListChunk(new HeapBufferSource(), -1);
      Assert.fail();
    } catch (IllegalStateException e) {
    }

    try {
      new BitSetLongAddressListChunk(new HeapBufferSource(), 0);
      Assert.fail();
    } catch (IllegalStateException e) {
    }

    try {
      new BitSetLongAddressListChunk(new HeapBufferSource(), 31);
      Assert.fail();
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void testCreationGood() {
    BitSetLongAddressListChunk bs = new BitSetLongAddressListChunk(new HeapBufferSource(), 10);
    Assert.assertThat(bs.getSlotCount(), is(1024));

    bs = new BitSetLongAddressListChunk(new HeapBufferSource(), 1);
    Assert.assertThat(bs.getSlotCount(), is(2));
  }

  @Test
  public void testAllSetGets() {
    BitSetLongAddressListChunk bs = new BitSetLongAddressListChunk(new HeapBufferSource(), 10);
    for (int i = 0; i < bs.getSlotCount(); i++) {
      bs.setToValue(i, i * 2);
    }
    for (int i = 0; i < bs.getSlotCount(); i++) {
      Assert.assertThat(bs.getSetValue(i), is((long) (i * 2)));
    }
  }

  @Test
  public void testAllClearGets() {
    BitSetLongAddressListChunk bs = new BitSetLongAddressListChunk(new HeapBufferSource(), 10);
    for (int i = 0; i < bs.getSlotCount(); i++) {
      Assert.assertThat(bs.getSetValue(i), lessThan(0l));
    }
  }
}
