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

import java.util.function.Predicate;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuildableOptionalFunctionTest {

  @Test
  public void testIsConstant() {
    BuildableOptionalFunction<Object, Object> function = t -> of(t);

    Predicate<Object> predicate = function.is("foo");

    assertFalse(predicate.test("bar"));
    assertTrue(predicate.test(new String("foo")));
  }

  @Test
  public void testBinaryIsConstant() {
    BuildableOptionalFunction<byte[], byte[]> function = t -> of(t);

    Predicate<byte[]> predicate = function.is(new byte[] {1, 2, 3, 4});

    assertFalse(predicate.test(new byte[] {1, 2, 3}));
    assertTrue(predicate.test(new byte[] {1, 2, 3, 4}));
  }

  @Test
  public void testEmptyIsConstant() {
    BuildableOptionalFunction<Object, Object> function = t -> empty();

    Predicate<Object> predicate = function.is("foo");

    assertFalse(predicate.test("foo"));
  }
}
