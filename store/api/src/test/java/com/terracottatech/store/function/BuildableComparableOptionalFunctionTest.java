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
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Optional.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuildableComparableOptionalFunctionTest {

  @Test
  public void testIsGreaterThanConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isGreaterThan(1);

    assertFalse(predicate.test(0));
    assertFalse(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testEmptyIsGreaterThanConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = t -> empty();

    Predicate<Integer> predicate = function.isGreaterThan(1);

    assertFalse(predicate.test(2));
  }

  @Test
  public void testIsLessThanConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isLessThan(1);

    assertTrue(predicate.test(0));
    assertFalse(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testEmptyIsLessThanConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = t -> empty();

    Predicate<Integer> predicate = function.isLessThan(1);

    assertFalse(predicate.test(0));
  }

  @Test
  public void testIsGreaterThanOrEqualToConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isGreaterThanOrEqualTo(1);

    assertFalse(predicate.test(0));
    assertTrue(predicate.test(1));
    assertTrue(predicate.test(2));
  }

  @Test
  public void testEmptyIsGreaterThanOrEqualToConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = t -> empty();

    Predicate<Integer> predicate = function.isGreaterThanOrEqualTo(1);

    assertFalse(predicate.test(2));
  }

  @Test
  public void testIsLessThanOrEqualToConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isLessThanOrEqualTo(1);

    assertTrue(predicate.test(0));
    assertTrue(predicate.test(1));
    assertFalse(predicate.test(2));
  }

  @Test
  public void testEmptyIsLessThanOrEqualToConstant() {
    BuildableComparableOptionalFunction<Integer, Integer> function = t -> empty();

    Predicate<Integer> predicate = function.isLessThanOrEqualTo(1);

    assertFalse(predicate.test(0));
  }

  @Test
  public void testAndConjuction() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isGreaterThan(10).and(function.isLessThan(20));

    assertFalse(predicate.test(4));
    assertTrue(predicate.test(15));
    assertFalse(predicate.test(31));
  }

  @Test
  public void testOrDisjunction() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isGreaterThan(20).or(function.isLessThan(10));

    assertTrue(predicate.test(4));
    assertFalse(predicate.test(15));
    assertTrue(predicate.test(31));
  }

  @Test
  public void testNotNegation() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    Predicate<Integer> predicate = function.isGreaterThan(20).negate();

    assertTrue(predicate.test(15));
    assertTrue(predicate.test(20));
    assertFalse(predicate.test(25));
  }

  @Test
  public void testVeryComplexPredicate() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    final Predicate<Integer> predicate1 = function.isGreaterThan(10).and(function.isLessThan(20));
    final Predicate<Integer> predicate2 = function.isGreaterThan(100).and(function.isLessThan(200));
    final Predicate<Integer> predicate3 = function.isGreaterThan(1000).and(function.isLessThan(2000));

    final Predicate<Integer> predicate4 = predicate1.or(predicate2).or(predicate3);
    final Predicate<Integer> predicate5 = predicate4.negate().or(predicate1);

    assertFalse(predicate4.test(4));
    assertTrue(predicate4.test(15));
    assertFalse(predicate4.test(31));
    assertTrue(predicate4.test(101));

    assertTrue(predicate5.test(4));
    assertTrue(predicate5.test(15));
    assertTrue(predicate5.test(31));
    assertFalse(predicate5.test(101));
  }

  @Test
  public void testVeryComplexPredicateConcurrent() {
    BuildableComparableOptionalFunction<Integer, Integer> function = Optional::of;

    final Predicate<Integer> predicate1 = function.isGreaterThan(10).and(function.isLessThan(20));
    final Predicate<Integer> predicate2 = function.isGreaterThan(100).and(function.isLessThan(200));
    final Predicate<Integer> predicate3 = function.isGreaterThan(1000).and(function.isLessThan(2000));

    final Runnable r1 = () -> checkPredicateInALoop(() -> predicate1.or(predicate2).or(predicate3),
        (x) -> (x > 10 && x < 20) || (x > 100 && x < 200) || (x > 1000 && x < 2000),
        "((((var>10)&&(var<20))||((var>100)&&(var<200)))||((var>1000)&&(var<2000)))",
        2000);

    final Runnable r2 = () -> checkPredicateInALoop(() -> predicate1.and(function.isLessThan(15).or(predicate3)),
        (x) -> (x > 10 && x < 20) && (x < 15 || (x > 1000 && x < 2000)),
        "(((var>10)&&(var<20))&&((var<15)||((var>1000)&&(var<2000))))",
        2000);

    Thread[] r1Threads = new Thread[100];
    Thread[] r2Threads = new Thread[100];

    for (int i = 0; i < 100; i++) {
      r1Threads[i] = new Thread(r1);
      r2Threads[i] = new Thread(r2);
      r1Threads[i].start();
      r2Threads[i].start();
    }

    // let the main test thread also verify a predicate which references the stored predicates above..
    checkPredicateInALoop(() -> (predicate1.or(predicate2).or(predicate3)).negate().or(predicate1),
        (x) -> !((x > 10 && x < 20) || (x > 100 && x < 200) || (x > 1000 && x < 2000)) || (x > 10 && x < 20),
        "((!((((var>10)&&(var<20))||((var>100)&&(var<200)))||((var>1000)&&(var<2000))))||((var>10)&&(var<20)))",
        2000);

    // Now join the threads
    for (int i = 0; i < 100; i++) {
      try {
        r1Threads[i].join();
        r2Threads[i].join();
      } catch (InterruptedException ignored) {
      }
    }
  }

  /**
   * Method that verifies that complex formations of predicate logic with a combination of
   * conjunction, disjunction, negation in Terracotta Store (i.e the comparison object that is a specialization of
   * the predicate) evaluates to the same result (regardless of the input) as the corresponding
   * boolean logic directly evaluated in java.
   *
   * The input values that are tested is generated at random.
   *
   * This method can be executed in parallel across multiple threads, in order to test thread safety.
   *
   * @param predicateSupplier the supplier function that returns a complex predicate that is created through TC Store api
   * @param expected a function that evaluates a similar boolean expression evaluated directly in java. This is
   *                 the expected result and it should match the actual {@code predicate.test(..)}
   * @param stringRep string representation of the expression to test the correctness of parse tree.
   */
  private void checkPredicateInALoop(Supplier<Predicate<Integer>> predicateSupplier,
                                     Function<Integer, Boolean> expected,
                                     String stringRep,
                                     int upperBound) {
    Random rand = new Random(Thread.currentThread().getId());
    for (int i = 0; i < 1000; i++) {
      Predicate<Integer> predicate = predicateSupplier.get();
      int j = rand.nextInt(upperBound);
      assertEquals(expected.apply(j), predicate.test(j));
      if (i%3 == 0) {
        Thread.yield();
      }
    }
  }
}
