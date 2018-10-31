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

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Currently used only for testing in various modules.
 */
public class Combinatorics {


  /**
   * Returns all possible orderings.
   *
   * @param items Array in an arbitrary order.
   * @return Stream of arrays of all possible orderings of elements in the input array.
   */
  @SuppressWarnings("varargs")
  @SafeVarargs
  public static <T> Stream<T[]> allOrderings(T... items) {
    if (items.length <= 1) {
      return Stream.<T[]>builder().add(items).build();
    } else {
      T zero = items[0];
      T[] tail = Arrays.copyOfRange(items, 1, items.length);

      return allOrderings(tail).flatMap(t -> IntStream.range(0, t.length + 1).mapToObj(i -> {
        T[] result = Arrays.copyOf(items, t.length + 1);
        System.arraycopy(t, 0, result, 0, i);
        result[i] = zero;
        System.arraycopy(t, i, result, i + 1, t.length - i);
        return result;
      }));
    }
  }
}
