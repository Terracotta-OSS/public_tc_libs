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

package com.terracottatech.store.logic;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link Bound}
 *
 * @param <V> type of the endpoint.
 */
abstract class TestBounds<V extends Comparable<V>> {

  protected abstract Start<V>[] getStartsInAscendingOrder();

  /**
   * Test {@link Start#equals(java.lang.Object)} by asserting that
   * each Start is equal to another Start with the same value and inclusion.
   */
  @Test
  public void testStartsEquality() {
    Stream.of(getStartsInAscendingOrder()).forEach(p -> {
      Start<V> other = new Start<>(p.getValue(), p.getInclusion());
      assertEquals(p, other);
    });
  }

  /**
   * Test {@link Start#compareTo(com.terracottatech.store.logic.Start)}
   * by asserting that all orderings of Starts are sorted into the pre-defined ascending order.
   */
  @Test
  public void testStartsInAscendingOrder() {
    Combinatorics.allOrderings(getStartsInAscendingOrder())
            .peek(Arrays::sort)
            .forEach(order -> Assert.assertArrayEquals(getStartsInAscendingOrder(), order));
  }

  protected abstract End<V>[] getEndsInAscendingOrder();

  /**
   * Test {@link End#equals(java.lang.Object)} by asserting that
   * any End is equal to another End with the same value and inclusion.
   */
  @Test
  public void testEndsEquality() {
    Stream.of(getEndsInAscendingOrder()).forEach(p -> {
      End<V> other = new End<>(p.getValue(), p.getInclusion());
      assertEquals(p, other);
    });
  }


  /**
   * Test {@link End#compareTo(com.terracottatech.store.logic.End)}
   * by asserting that all orderings of Ends are sorted into the pre-defined ascending order.
   */
  @Test
  public void testEndsInAscendingOrder() {
    Combinatorics.allOrderings(getEndsInAscendingOrder())
            .peek(Arrays::sort)
            .forEach(order -> Assert.assertArrayEquals(getEndsInAscendingOrder(), order));
  }
}
