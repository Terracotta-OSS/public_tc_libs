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
package com.terracottatech.store.function;

import org.junit.Test;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BuildableStringFunctionTest {

  @Test
  public void testLength() {
    BuildableStringFunction<String> function = t -> t;

    @SuppressWarnings("deprecation")
    ToIntFunction<String> length = function.length();

    assertEquals(length.applyAsInt(""), 0);
    assertEquals(length.applyAsInt("foo"), 3);
  }

  @Test
  public void testStartsWithConstant() {
    BuildableStringFunction<String> function = t -> t;

    Predicate<String> startsWith = function.startsWith("fo");

    assertTrue(startsWith.test("foo"));
    assertFalse(startsWith.test("bar"));
  }

  @Test
  public void testAsComparator() {
    BuildableStringFunction<String> function = t -> t;

    Comparator<String> comparator = function.asComparator();

    assertThat(comparator.compare("a", "b"), lessThan(0));
    assertThat(comparator.compare("a", "a"), is(0));
    assertThat(comparator.compare("b", "a"), greaterThan(0));
  }

  @Test
  public void testAsComparatorReverse() {
    BuildableStringFunction<String> function = t -> t;

    Comparator<String> comparator = function.asComparator().reversed();

    assertThat(comparator.compare("a", "b"), greaterThan(0));
    assertThat(comparator.compare("a", "a"), is(0));
    assertThat(comparator.compare("b", "a"), lessThan(0));
  }

  @Test
  public void testAsComparatorReserveReverse() {
    BuildableStringFunction<String> function = t -> t;

    Comparator <String> comparator = function.asComparator().reversed().reversed();

    assertThat(comparator.compare("a", "b"), lessThan(0));
    assertThat(comparator.compare("a", "a"), is(0));
    assertThat(comparator.compare("b", "a"), greaterThan(0));
  }
}
