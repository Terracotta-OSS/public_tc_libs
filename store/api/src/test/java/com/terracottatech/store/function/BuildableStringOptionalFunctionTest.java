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

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuildableStringOptionalFunctionTest {

  @Test
  public void testLength() {
    BuildableStringOptionalFunction<String> function = t -> of(t);

    Function<String, Optional<Integer>> length = function.length();

    assertEquals(length.apply(""), of(0));
    assertEquals(length.apply("foo"), of(3));
  }

  @Test
  public void testEmptyLength() {
    BuildableStringOptionalFunction<String> function = t -> empty();

    Function<String, Optional<Integer>> length = function.length();

    assertEquals(length.apply(""), empty());
  }

  @Test
  public void testStartsWithConstant() {
    BuildableStringOptionalFunction<String> function = t -> of(t);

    Predicate<String> startsWith = function.startsWith("fo");

    assertTrue(startsWith.test("foo"));
    assertFalse(startsWith.test("bar"));
  }
}
