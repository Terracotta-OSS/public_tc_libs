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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildableOptionalFunction;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class FunctionsTest {

  @Test
  public void testRecordEqualsPredicate() throws Exception {
    Record<String> record = new TestRecord<>("bob", Collections.emptyList());
    Record<String> equalRecord = new TestRecord<>("bob", Collections.emptyList());
    Record<String> sameCellsDifferentKeyRecord = new TestRecord<>("joe", Collections.emptyList());
    Record<String> differentCellsSameKeyRecord = new TestRecord<>("bob", Collections.singleton(CellDefinition.defineInt("bla").newCell(1)));

    Predicate<Record<String>> recordPredicate = new RecordEquality<>(record);

    RecordEquality<?> intrinsic = (RecordEquality) recordPredicate;
    assertThat(intrinsic.getRecord(), is(record));
    assertThat(intrinsic.getIntrinsicType(), Is.is(IntrinsicType.PREDICATE_RECORD_EQUALS));

    assertThat(recordPredicate.test(equalRecord), is(true));
    assertThat(recordPredicate.test(sameCellsDifferentKeyRecord), is(false));
    assertThat(recordPredicate.test(differentCellsSameKeyRecord), is(false));
  }

  @Test
  public void testAlwaysTrue() throws Exception {
    Record<String> record = new TestRecord<>("bob", Collections.emptyList());
    Record<String> differentRecord = new TestRecord<>("joe", Collections.singleton(CellDefinition.defineInt("bla").newCell(1)));

    Predicate<Record<?>> recordPredicate = AlwaysTrue.alwaysTrue();
    Intrinsic intrinsic = (Intrinsic) recordPredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_ALWAYS_TRUE));

    assertThat(recordPredicate.test(record), is(true));
    assertThat(recordPredicate.test(differentRecord), is(true));
  }

  @Test
  public void testCellDefinitionExists() throws Exception {
    Record<String> record = new TestRecord<>("bob", Collections.emptyList());
    Record<String> differentRecord = new TestRecord<>("joe", Collections.singleton(CellDefinition.defineInt("bla").newCell(1)));

    Predicate<Record<?>> cellDefinitionPredicate = Functions.cellDefinitionExists(CellDefinition.defineInt("bla"));
    assertThat(((Intrinsic) cellDefinitionPredicate).getIntrinsicType(), is(IntrinsicType.PREDICATE_CELL_DEFINITION_EXISTS));

    assertThat(cellDefinitionPredicate.test(record), is(false));
    assertThat(cellDefinitionPredicate.test(differentRecord), is(true));
  }

  @Test
  public void testInstallUpdateOperation() throws Exception {
    UpdateOperation<String> updateOperation = Functions.installUpdateOperation(Collections.singleton(CellDefinition.defineInt("bla").newCell(1)));

    InstallOperation<?> intrinsic = (InstallOperation) updateOperation;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.UPDATE_OPERATION_INSTALL));
    assertThat(intrinsic.getCells(), is(Collections.singleton(CellDefinition.defineInt("bla").newCell(1))));

    Iterable<Cell<?>> cellIterable = updateOperation.apply(null);
    List<Cell<?>> cells = new ArrayList<>();
    cellIterable.forEach(cells::add);

    assertThat(cells, is(Collections.singletonList(CellDefinition.defineInt("bla").newCell(1))));
  }

  @Test
  public void testExtractPojo() throws Exception {
    IntCellDefinition definition = CellDefinition.defineInt("bla");
    BuildableOptionalFunction<Record<?>, Integer> extractor = Functions.extractPojo(definition);

    CellExtractor<?> intrinsic = (CellExtractor<?>) extractor;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.FUNCTION_CELL));
    assertThat(intrinsic.extracts(), is(definition));

    Record<String> record = new TestRecord<>("bob", asList(definition.newCell(42)));
    assertThat(extractor.apply(record), is(Optional.of(42)));
  }

  @Test
  public void testExtractComparable() throws Exception {
    IntCellDefinition definition = CellDefinition.defineInt("bla");
    BuildableOptionalFunction<Record<?>, Integer> extractor = Functions.extractComparable(definition);

    CellExtractor<?> intrinsic = (CellExtractor<?>) extractor;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.FUNCTION_CELL));
    assertThat(intrinsic.extracts(), is(definition));

    Record<String> record = new TestRecord<>("bob", asList(definition.newCell(42)));
    assertThat(extractor.apply(record), is(Optional.of(42)));
  }

  @Test
  public void testExtractString() throws Exception {
    StringCellDefinition definition = CellDefinition.defineString("bla");
    BuildableOptionalFunction<Record<?>, String> extractor = Functions.extractString(definition);

    CellExtractor<?> intrinsic = (CellExtractor<?>) extractor;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.FUNCTION_CELL));
    assertThat(intrinsic.extracts(), is(definition));

    Record<String> record = new TestRecord<>("bob", asList(definition.newCell("alice")));
    assertThat(extractor.apply(record), is(Optional.of("alice")));
  }

  @Test
  public void testAndPredicateChainedBuild() throws Exception {
    IntCellDefinition ageCell = CellDefinition.defineInt("age");
    StringCellDefinition nameCell = CellDefinition.defineString("name");

    Predicate<Record<?>> agePredicate = Functions.extractComparable(ageCell).is(21);
    Predicate<Record<?>> namePredicate = Functions.extractString(nameCell).is("joe");
    Predicate<Record<?>> alwaysTruePredicate = AlwaysTrue.alwaysTrue();
    Predicate<Record<?>> ageAndNamePredicate = agePredicate.and(namePredicate);
    Predicate<Record<?>> andPredicate = ageAndNamePredicate.and(alwaysTruePredicate);

    BinaryBoolean.And<?> intrinsic = (BinaryBoolean.And<?>) andPredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_AND));
    IntrinsicPredicate<?> left = intrinsic.getLeft();
    assertThat(left, is(ageAndNamePredicate));
    assertThat(intrinsic.getRight(), is(alwaysTruePredicate));

    assertThat(((BinaryBoolean.And<?>) left).getLeft(), is(agePredicate));
    assertThat(((BinaryBoolean.And<?>) left).getRight(), is(namePredicate));

    assertThat(andPredicate.test(new TestRecord<>("recordKey", asList(ageCell.newCell(21), nameCell.newCell("joe")))), is(true));
    assertThat(andPredicate.test(new TestRecord<>("recordKey", asList(ageCell.newCell(21), nameCell.newCell("jack")))), is(false));
    assertThat(andPredicate.test(new TestRecord<>("recordKey", asList(ageCell.newCell(22), nameCell.newCell("joe")))), is(false));
    assertThat(andPredicate.test(new TestRecord<>("recordKey", asList(ageCell.newCell(22), nameCell.newCell("jack")))), is(false));
    assertThat(andPredicate.test(new TestRecord<>("recordKey", asList())), is(false));
  }

  @Test
  public void testOrPredicateChainedBuild() throws Exception {
    IntCellDefinition ageCell = CellDefinition.defineInt("age");
    StringCellDefinition nameCell = CellDefinition.defineString("name");
    StringCellDefinition notCell = CellDefinition.defineString("_mustNotExist_");

    Predicate<Record<?>> agePredicate = Functions.extractComparable(ageCell).is(21);
    Predicate<Record<?>> namePredicate = Functions.extractString(nameCell).is("joe");

    Predicate<Record<?>> alwaysFalsePredicate = Functions.extractPojo(notCell).is("foo");
    Predicate<Record<?>> ageOrNamePredicate = agePredicate.or(namePredicate);
    Predicate<Record<?>> orPredicate = ageOrNamePredicate.or(alwaysFalsePredicate);

    BinaryBoolean.Or<?> intrinsic = (BinaryBoolean.Or<?>) orPredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_OR));
    IntrinsicPredicate<?> left = intrinsic.getLeft();
    assertThat(left, is(ageOrNamePredicate));
    assertThat(intrinsic.getRight(), is(alwaysFalsePredicate));

    assertThat(((BinaryBoolean.Or<?>) left).getLeft(), is(agePredicate));
    assertThat(((BinaryBoolean.Or<?>) left).getRight(), is(namePredicate));

    assertThat(orPredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(21), CellDefinition.defineString("name").newCell("joe")))), is(true));
    assertThat(orPredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(21), CellDefinition.defineString("name").newCell("jack")))), is(true));
    assertThat(orPredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(22), CellDefinition.defineString("name").newCell("joe")))), is(true));
    assertThat(orPredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(22), CellDefinition.defineString("name").newCell("jack")))), is(false));
    assertThat(orPredicate.test(new TestRecord<>("recordKey", asList())), is(false));
  }

  @Test
  public void testCellValueEqualsPredicate() throws Exception {
    IntCellDefinition ageCell = CellDefinition.defineInt("age");
    Predicate<Record<?>> agePredicate = Functions.extractComparable(ageCell).is(21);

    GatedComparison.Equals<?, ?> intrinsic = (GatedComparison.Equals<?, ?>) agePredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_GATED_EQUALS));
    assertThat(((CellExtractor<?>) intrinsic.getLeft()).extracts(), is(ageCell));
    assertThat(((Constant<?, ?>) intrinsic.getRight()).getValue(), is(21));

    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(21)))), is(true));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("streetNum").newCell(21)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(22)))), is(false));
  }

  @Test
  public void testCellValueGreaterThanPredicate() throws Exception {
    IntCellDefinition ageCell = CellDefinition.defineInt("age");
    Predicate<Record<?>> agePredicate = Functions.extractComparable(ageCell).isGreaterThan(21);

    GatedComparison.Contrast<?, ?> intrinsic = (GatedComparison.Contrast<?, ?>) agePredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_GATED_CONTRAST));
    assertThat(((CellExtractor<?>) intrinsic.getLeft()).extracts(), is(ageCell));
    assertThat(((Constant<?, ?>) intrinsic.getRight()).getValue(), is(21));
    assertThat(intrinsic.getComparisonType(), is(GREATER_THAN));

    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(2)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(21)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("streetNum").newCell(21)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(22)))), is(true));
  }

  @Test
  public void testCellValueLessThanPredicate() throws Exception {
    IntCellDefinition ageCell = CellDefinition.defineInt("age");
    Predicate<Record<?>> agePredicate = Functions.extractComparable(ageCell).isLessThan(21);

    GatedComparison.Contrast<?, ?> intrinsic = (GatedComparison.Contrast<?, ?>) agePredicate;
    assertThat(intrinsic.getIntrinsicType(), is(IntrinsicType.PREDICATE_GATED_CONTRAST));
    assertThat(((CellExtractor<?>) intrinsic.getLeft()).extracts(), is(ageCell));
    assertThat(((Constant<?, ?>) intrinsic.getRight()).getValue(), is(21));
    assertThat(intrinsic.getComparisonType(), is(LESS_THAN));

    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(2)))), is(true));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(21)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("streetNum").newCell(21)))), is(false));
    assertThat(agePredicate.test(new TestRecord<>("recordKey", asList(CellDefinition.defineInt("age").newCell(22)))), is(false));
  }
}
