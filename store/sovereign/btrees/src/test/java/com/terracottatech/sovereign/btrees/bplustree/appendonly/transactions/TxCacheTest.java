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

package com.terracottatech.sovereign.btrees.bplustree.appendonly.transactions;

import com.terracottatech.sovereign.btrees.bplustree.TxCache;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TxCacheTest {

  @Test
  public void testLookupHitMiss() {
    TxCache<Node> cache = new TxCache<>(10);

    Node n1 = mock(Node.class);
    when(n1.getOffset()).thenReturn(100l);
    Node n2 = mock(Node.class);
    when(n2.getOffset()).thenReturn(200l);
    cache.cache(n1);

    Assert.assertThat(cache.getNode(n1.getOffset()), is(n1));
    Assert.assertNull(cache.getNode(n2.getOffset()));

  }

  @Test
  public void testOverflow() {
    Tx<?> tx = mock(Tx.class);

    TxCache<Node> cache = new TxCache<>(10);

    for (int i = 0; i < 10; i++) {
      Node n1 = mock(Node.class);
      when(n1.getOffset()).thenReturn((long) i);
      cache.cache(n1);
    }
    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(cache.getNode(i));
    }
    // LRU should be 0
    Node n1 = mock(Node.class);
    when(n1.getOffset()).thenReturn(100L);
    cache.cache(n1);
    for (int i = 1; i < 10; i++) {
      Assert.assertNotNull(cache.getNode(i));
    }
    Assert.assertNull(cache.getNode(0));
    Assert.assertNotNull(cache.getNode(100));

  }

  @Test
  public void testInvalidation() {
    Tx<?> tx = mock(Tx.class);

    TxCache<Node> cache = new TxCache<>(10);

    for (int i = 0; i < 10; i++) {
      Node n1 = mock(Node.class);
      when(n1.getOffset()).thenReturn((long) i);
      cache.cache(n1);
    }
    cache.invalidate(9);
    Assert.assertNull(cache.getNode(9));
  }
}
