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

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.NEQ;
import static com.terracottatech.store.logic.NormalForm.Type.CONJUNCTIVE;
import static com.terracottatech.store.logic.NormalForm.Type.DISJUNCTIVE;
import static org.junit.Assert.assertEquals;

/**
 * Testing {@link NormalFormCompactor}.
 */
public class NormalFormCompactorTest {

  /**
   * CNF range compaction not supported.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testCnf() {
    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(CONJUNCTIVE).build();
    compactNF(dnf);
  }

  /**
   * false -> false
   */
  @Test
  public void testContradiction() {
    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = constant(false);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compact = compactNF(dnf);
    assertEquals(dnf, compact);
  }

  /**
   * true -> true
   */
  @Test
  public void testTautology() {
    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = constant(true);
    NormalForm<Record<Integer>, ? extends Predicate<Record<Integer>>> compact = compactNF(dnf);
    assertEquals(dnf, compact);
  }

  /**
   * (x1 < 1) & (x2 > 1) -> itself
   */
  @Test
  public void testDifferentCells() {

    IntCellDefinition def1 = CellDefinition.defineInt("test2");
    IntrinsicPredicate<Record<Integer>> lt1 = gatedComparison(def1, LESS_THAN, -1);

    IntCellDefinition def2 = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> gt2 = gatedComparison(def2, GREATER_THAN, 1);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(lt1, gt2)
            .build();

    NormalForm<Record<Integer>, RecordPredicate<Integer>> compact = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(
                    new IntervalRecordPredicate<>(def1, intervalBuilder().endClosed(-1).build()),
                    new IntervalRecordPredicate<>(def2, intervalBuilder().startClosed(1).build())
            )
            .build();
    assertEquals(expected, compact);
  }


  /**
   * (x < 1) | (x > 1) -> itself
   */
  @Test
  public void testDifferentClauses() {
    IntCellDefinition def = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 1);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 1);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(lt)
            .addClause(gt)
            .build();

    NormalForm<Record<Integer>, ? extends Predicate<Record<Integer>>> compact = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().startClosed(1).build()))
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().endClosed(1).build()))
            .build();
    assertEquals(expected, compact);
  }

  /**
   * (x1 < 1) & (x1 > 1) -> false
   */
  @Test
  public <V extends Comparable<V>> void testNegativeRange() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 1);
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, -1);
    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(gt, lt)
            .build();

    NormalForm<Record<Integer>, RecordPredicate<Integer>> compact = compactNF(dnf);

    NormalForm<Record<V>, IntrinsicPredicate<Record<V>>> expected = constant(false);
    assertEquals(expected, compact);
  }

  /**
   * (x1 < 1) & (x1 > 1) & (x2 = "X") -> false
   */
  @Test
  public <V extends Comparable<V>> void testNegativeRangeWithAnotherCell() {
    IntCellDefinition def1 = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> gt1 = gatedComparison(def1, GREATER_THAN, 1);
    IntrinsicPredicate<Record<Integer>> lt1 = gatedComparison(def1, LESS_THAN, -1);

    StringCellDefinition def2 = CellDefinition.defineString("test2");
    IntrinsicPredicate<Record<Integer>> eq2 = gatedComparison(def2, EQ, "X");

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(gt1, lt1, eq2)
            .build();
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(dnf);

    NormalForm<Record<V>, IntrinsicPredicate<Record<V>>> expectedForm = constant(false);
    assertEquals(expectedForm, compactDnf);
  }

  /**
   * (x1 > -1) & (x > 0) & (x1 < 1) & (x < 2) -> x in (0, 1]
   */
  @Test
  public void testPositiveRange() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, -1);
    IntrinsicPredicate<Record<Integer>> gte = gatedComparison(def, GREATER_THAN_OR_EQUAL, 0);
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 1);
    IntrinsicPredicate<Record<Integer>> lte = gatedComparison(def, LESS_THAN_OR_EQUAL, 2);
    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(gt, gte, lt, lte)
            .build();

    NormalForm<Record<Integer>, RecordPredicate<Integer>> compact = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().startOpen(0).endClosed(1).build()))
            .build();
    assertEquals(expected, compact);
  }

  /**
   * (x1 > -1) & (x1 < 1) & (x2 exists) -> (x1 in [-1, 1]) & (x2 exists)
   */
  @Test
  public void testPositiveRangeWithAnotherCell() {
    IntCellDefinition def1 = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> gt1 = gatedComparison(def1, GREATER_THAN, -1);
    IntrinsicPredicate<Record<Integer>> lt1 = gatedComparison(def1, LESS_THAN, 1);

    StringCellDefinition def2 = CellDefinition.defineString("test2");
    IntrinsicPredicate<Record<Integer>> eq2 = gatedComparison(def2, EQ, "X");

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(gt1, lt1, eq2)
            .build();
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(
                    new IntervalRecordPredicate<>(def1, intervalBuilder().startClosed(-1).endClosed(1).build()),
                    new IntervalRecordPredicate<>(def2, Interval.<String>builder().startOpen("X").endOpen("X").build())
            )
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * exists() is currently not converted to an interval.
   */
  @Test
  public void testExists() {
    StringCellDefinition def = CellDefinition.defineString("test");
    IntrinsicPredicate<Record<Integer>> exists = exists(def);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(exists)
            .build();
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new DefaultRecordPredicate<>(exists))
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * ((x1 > -1) & (x1 >= 0) & (x1 < 1) & (x1 <= 2) & (x2 exists)) | ((x1 > -1) & (x1 <= 2) & (x2 = 'X')) =>
   * ((x 1 in (0, 1]) & (x2 exists)) | ((x1 in [-1, 2)) & x2 in ('X', 'X'))
   */
  @Test
  public void testDisjunctionOfConjunctions() {
    IntCellDefinition def1 = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def1, GREATER_THAN, -1);
    IntrinsicPredicate<Record<Integer>> gte = gatedComparison(def1, GREATER_THAN_OR_EQUAL, 0);
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def1, LESS_THAN, 1);
    IntrinsicPredicate<Record<Integer>> lte = gatedComparison(def1, LESS_THAN_OR_EQUAL, 2);

    StringCellDefinition def2 = CellDefinition.defineString("test2");
    IntrinsicPredicate<Record<Integer>> exists2 = exists(def2);
    IntrinsicPredicate<Record<Integer>> eq2 = gatedComparison(def2, EQ, "X");

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = normalFormBuilder(DISJUNCTIVE)
            .addClause(gt, gte, lt, lte, exists2)
            .addClause(gt, lte, eq2)
            .build();
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(
                    new IntervalRecordPredicate<>(def1, intervalBuilder().startOpen(0).endClosed(1).build()),
                    new DefaultRecordPredicate<>(exists2)
            )
            .addClause(
                    new IntervalRecordPredicate<>(def1, intervalBuilder().startClosed(-1).endOpen(2).build()),
                    new IntervalRecordPredicate<>(def2, Interval.<String>builder().startOpen("X").endOpen("X").build())
            )
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * (x != 10) => (x < 10) | (x > 10)
   */
  @Test
  public void testNotEquals() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> neq = gatedComparison(def, NEQ, 10);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> dnf = toNormalForm(neq);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(dnf);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().endClosed(10).build()))
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().startClosed(10).build()))
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * For a comparable x:
   *
   * (x = 10) & (x != 10) => false
   */
  @Test
  public void testEqualsAndNotEquals() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> eq = gatedComparison(def, EQ, 10);
    IntrinsicPredicate<Record<Integer>> neq = gatedComparison(def, NEQ, 10);
    BinaryBoolean.And<Record<Integer>> and = new BinaryBoolean.And<>(eq, neq);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(and);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = NormalForm.constant(DISJUNCTIVE, false);
    assertEquals(expected, compactDnf);
  }

  /**
   * (x = 10) & (x != 10) => false
   */
  @Test
  public void testBytesEqualsAndNotEquals() {
    BytesCellDefinition def = CellDefinition.defineBytes("test");
    byte[] value = {};
    IntrinsicPredicate<Record<Integer>> eq = gatedEquals(def, EQ, value);
    IntrinsicPredicate<Record<Integer>> neq = gatedEquals(def, NEQ, value);
    BinaryBoolean.And<Record<Integer>> and = new BinaryBoolean.And<>(eq, neq);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(and);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    assertEquals(NormalForm.constant(DISJUNCTIVE, false), compactDnf);
  }


  /**
   * (x < 10) | (x > 0) => x in (-inf, inf)
   */
  @Test
  public void testInfiniteUnion() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 10);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 0);
    BinaryBoolean.Or<Record<Integer>> or = new BinaryBoolean.Or<>(lt, gt);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(or);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = NormalForm.constant(DISJUNCTIVE, true);
    assertEquals(expected, compactDnf);
  }

  /**
   * ((x >= 0) & (x < 10)) | ((x > 5) & (x <= 20)) => x in (0, 20)
   */
  @Test
  public void testFiniteUnion() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> gte = gatedComparison(def, GREATER_THAN_OR_EQUAL, 0);
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 10);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 5);
    IntrinsicPredicate<Record<Integer>> lte = gatedComparison(def, LESS_THAN_OR_EQUAL, 20);
    BinaryBoolean.Or<Record<Integer>> or = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(gte, lt),
            new BinaryBoolean.And<>(gt, lte)
    );

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(or);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().startOpen(0).endOpen(20).build()))
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * (x >= 0) | (x < 10)) | (x > 5) | (x <= 20) => x in (inf, inf)
   */
  @Test
  public void testInfiniteUnionOfFourInequalities() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> gte = gatedComparison(def, GREATER_THAN_OR_EQUAL, 0);
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 10);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 5);
    IntrinsicPredicate<Record<Integer>> lte = gatedComparison(def, LESS_THAN_OR_EQUAL, 20);
    BinaryBoolean.Or<Record<Integer>> or = new BinaryBoolean.Or<>(
            new BinaryBoolean.Or<>(gte, lt),
            new BinaryBoolean.Or<>(gt, lte)
    );

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(or);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = NormalForm.constant(DISJUNCTIVE, true);
    assertEquals(expected, compactDnf);
  }

  /**
   * (x < 10) | (x > 10) => itself
   */
  @Test
  public void testDisruptedUnion() {
    IntCellDefinition def = CellDefinition.defineInt("test");
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def, LESS_THAN, 10);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def, GREATER_THAN, 10);
    BinaryBoolean.Or<Record<Integer>> or = new BinaryBoolean.Or<>(lt, gt);

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(or);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().endClosed(10).build()))
            .addClause(new IntervalRecordPredicate<>(def, intervalBuilder().startClosed(10).build()))
            .build();
    assertEquals(expected, compactDnf);
  }

  /**
   * (x < 10) | ((x > 0) & (y = 1)) => itself
   */
  @Test
  public void testUnionImpossibleWithConjunction() {
    IntCellDefinition def1 = CellDefinition.defineInt("test1");
    IntrinsicPredicate<Record<Integer>> lt = gatedComparison(def1, LESS_THAN, 10);
    IntrinsicPredicate<Record<Integer>> gt = gatedComparison(def1, GREATER_THAN, 0);
    StringCellDefinition def2 = CellDefinition.defineString("test2");
    IntrinsicPredicate<Record<Integer>> eq = gatedComparison(def2, EQ, "val");
    BinaryBoolean.Or<Record<Integer>> or = new BinaryBoolean.Or<>(lt,
            new BinaryBoolean.And<>(gt, eq));

    NormalForm<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalForm = toNormalForm(or);
    NormalForm<Record<Integer>, RecordPredicate<Integer>> compactDnf = compactNF(normalForm);

    NormalForm<Record<Integer>, RecordPredicate<Integer>> expected = compactFormBuilder(DISJUNCTIVE)
            .addClause(new IntervalRecordPredicate<>(def1, intervalBuilder().endClosed(10).build()))
            .addClause(
                    new IntervalRecordPredicate<>(def1, intervalBuilder().startClosed(0).build()),
                    new IntervalRecordPredicate<>(def2,
                            Interval.<String>builder().startOpen("val").endOpen("val").build())
            )
            .build();
    assertEquals(expected, compactDnf);
  }

  private static NormalForm.Builder<Record<Integer>, IntrinsicPredicate<Record<Integer>>> normalFormBuilder(NormalForm.Type operation) {
    return NormalForm.builder(operation);
  }

  private static NormalForm.Builder<Record<Integer>, RecordPredicate<Integer>> compactFormBuilder(NormalForm.Type operation) {
    return NormalForm.builder(operation);
  }

  private static Interval.Builder<Integer> intervalBuilder() {
    return Interval.builder();
  }

  private static <V extends Comparable<V>> NormalForm<Record<V>, IntrinsicPredicate<Record<V>>> constant(boolean value) {
    return NormalForm.constant(DISJUNCTIVE, value);
  }

  @SuppressWarnings({"unchecked", "RedundantCast"})
  private static <K extends Comparable<K>, V extends Comparable<V>> IntrinsicPredicate<Record<K>> gatedComparison(
          CellDefinition<V> def, ComparisonType op, V val) {

    IntrinsicFunction<Record<K>, Optional<V>> cellExtractor =
            (IntrinsicFunction<Record<K>, Optional<V>>) (Object) CellExtractor.extractComparable(def);
    switch (op) {
      case EQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val));
      case NEQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val)).negate();
      default:
        return new GatedComparison.Contrast<>(cellExtractor, op, new Constant<>(val));
    }
  }

  @SuppressWarnings({"unchecked", "RedundantCast"})
  private static <K extends Comparable<K>, V> IntrinsicPredicate<Record<K>> gatedEquals(
          CellDefinition<V> def, ComparisonType op, V val) {

    IntrinsicFunction<Record<K>, Optional<V>> cellExtractor =
            (IntrinsicFunction<Record<K>, Optional<V>>) (Object) CellExtractor.extractPojo(def);
    switch (op) {
      case EQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val));
      case NEQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val)).negate();
      default:
        throw new IllegalArgumentException();
    }
  }

  private <K extends Comparable<K>> NormalForm<Record<K>, RecordPredicate<K>> compactNF(NormalForm<Record<K>, IntrinsicPredicate<Record<K>>> dnf) {
    return new NormalFormCompactor<K>().compactNF(dnf);
  }

  private <K extends Comparable<K>> NormalForm<Record<K>,IntrinsicPredicate<Record<K>>> toNormalForm(IntrinsicPredicate<Record<K>> predicate) {
    return new BooleanAnalyzer<Record<K>>().toNormalForm(predicate, DISJUNCTIVE);
  }

  @SuppressWarnings("unchecked")
  private IntrinsicPredicate<Record<Integer>> exists(StringCellDefinition def) {
    return (IntrinsicPredicate<Record<Integer>>) (Object) def.exists();
  }
}
