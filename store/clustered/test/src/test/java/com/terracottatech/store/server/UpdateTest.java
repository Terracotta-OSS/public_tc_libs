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
package com.terracottatech.store.server;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.store.Cell;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.terracottatech.store.UpdateOperation.install;
import static com.terracottatech.store.UpdateOperation.write;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class UpdateTest extends PassthroughTest {

  private final List<String> stripeNames;

  public UpdateTest(List<String> stripeNames) {
    this.stripeNames = stripeNames;
  }

  @Override
  protected List<String> provideStripeNames() {
    return stripeNames;
  }

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {Collections.singletonList("stripe"), Arrays.asList("stripe1", "stripe2") };
  }

  private static final IntCellDefinition STREET_NUM = CellDefinition.defineInt("streetNum");
  private static final IntCellDefinition ZIP_CODE = CellDefinition.defineInt("zipCode");
  private static final StringCellDefinition STREET_NAME = CellDefinition.defineString("streetName");
  private static final LongCellDefinition ID = CellDefinition.defineLong("id");
  private static final DoubleCellDefinition AVG = CellDefinition.defineDouble("avg");
  private static final BoolCellDefinition VALID = CellDefinition.defineBool("valid");

  @Test
  public void add() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    assertTrue(access.add("abc", ID.newCell(10L), STREET_NUM.newCell(-10), AVG.newCell(55.66)));

    // Update the record, with a remove intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(access.update("abc", UpdateOperation.write(ID).longResultOf(ID.longValueOrFail().increment())));
    verify(datasetEntity, times(0)).get(any());
    assertTrue(access.update("abc", UpdateOperation.write(STREET_NUM).intResultOf(STREET_NUM.intValueOrFail().decrement())));
    verify(datasetEntity, times(0)).get(any());
    assertTrue(access.update("abc", UpdateOperation.write(AVG).doubleResultOf(AVG.doubleValueOrFail().subtract(0.11))));
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    assertThat(access.get("abc").get().get(ID.name()).get(), is(11L));
    assertThat(access.get("abc").get().get(STREET_NUM.name()).get(), is(-11));
    assertThat(access.get("abc").get().get(AVG.name()).get(), is(55.55));
  }

  @Test
  public void removeCellInRecords() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    assertTrue(access.add("abc", STREET_NUM.newCell(10), ZIP_CODE.newCell(123456)));

    // Update the record, with a remove intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(access.update("abc", UpdateOperation.remove(STREET_NUM)));
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    assertThat(access.get("abc").get().get(STREET_NUM.name()).isPresent(), is(false));
    assertThat(access.get("abc").get().get(ZIP_CODE.name()).get(), is(123456));
  }

  @Test
  public void allOf() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    assertTrue(access.add("abc", STREET_NUM.newCell(10), ZIP_CODE.newCell(123456)));

    // Update the record, with a remove intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(access.update("abc", UpdateOperation.allOf(UpdateOperation.remove(STREET_NUM), write(STREET_NAME.newCell("some street")))));
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    assertThat(access.get("abc").get().get(STREET_NUM.name()).isPresent(), is(false));
    assertThat(access.get("abc").get().get(ZIP_CODE.name()).get(), is(123456));
    assertThat(access.get("abc").get().get(STREET_NAME.name()).get(), is("some street"));
  }

  @Test
  public void updateRecords() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    // record does not exist
    assertFalse(access.get("abc").isPresent());

    // update fails as the record doesn't exist
    assertFalse(access.update("abc", cells -> Collections.singleton(STREET_NUM.newCell(30))));

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now the record can be updated
    assertTrue(access.update("abc", cells -> Collections.singleton(STREET_NUM.newCell(30))));
    assertTrue(access.update("abc", cells -> Collections.singleton(STREET_NUM.newCell(40))));
    assertTrue(access.update("abc", cells -> Arrays.asList(STREET_NUM.newCell(45), ZIP_CODE.newCell(123456))));

    // check that the above updates did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // Check that all the updates had an effect on the data
    Optional<Record<String>> recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Arrays.asList(STREET_NUM.newCell(45), ZIP_CODE.newCell(123456))));

    // Update the record again, with an install intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(access.update("abc", install(Collections.singleton(STREET_NUM.newCell(50)))));
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(STREET_NUM.newCell(50))));

    // Update the record again, with a write intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(access.update("abc", write(STREET_NUM.name(), 60)));
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(STREET_NUM.newCell(60))));

    resetDatasetEntity();
    access.update("abc", UpdateOperation.write(STREET_NUM).resultOf(ZIP_CODE.valueOr(555)));
    verify(datasetEntity, times(0)).get(any());
    access.update("abc", UpdateOperation.write(ZIP_CODE).intResultOf(ZIP_CODE.intValueOr(-999)));
    verify(datasetEntity, times(0)).get(any());
    access.update("abc", UpdateOperation.write(ID).longResultOf(ID.longValueOr(-88888)));
    verify(datasetEntity, times(0)).get(any());
    access.update("abc", UpdateOperation.write(AVG).doubleResultOf(AVG.doubleValueOr(Double.NaN)));
    verify(datasetEntity, times(0)).get(any());
    access.update("abc", UpdateOperation.write(STREET_NAME).resultOf(STREET_NAME.valueOr("da street")));
    verify(datasetEntity, times(0)).get(any());
    access.update("abc", UpdateOperation.write(VALID).resultOf(VALID.valueOr(true)));
    verify(datasetEntity, times(0)).get(any());
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Arrays.asList(STREET_NUM.newCell(555), ZIP_CODE.newCell(-999), ID.newCell(-88888L), AVG.newCell(Double.NaN), STREET_NAME.newCell("da street"), VALID.newCell(true))));
  }

  @Test
  public void updateRecordsTuple() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    ReadWriteRecordAccessor<String> abcAccessor = access.on("abc");
    // record does not exist
    assertFalse(access.get("abc").isPresent());

    // update fails as the record doesn't exist
    assertFalse(abcAccessor.update(r -> Collections.singleton(STREET_NUM.newCell(30))).isPresent());

    // check that the above update did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // same as above but with an intrinsic UpdateOperation
    resetDatasetEntity();
    assertFalse(abcAccessor.update(install(Collections.singleton(STREET_NUM.newCell(30)))).isPresent());
    verify(datasetEntity, times(0)).get(any());

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now the record can be updated
    assertTrue(abcAccessor.update(r -> Collections.singleton(STREET_NUM.newCell(30))).isPresent());
    assertTrue(abcAccessor.update(r -> Collections.singleton(STREET_NUM.newCell(40))).isPresent());
    assertTrue(abcAccessor.update(r -> Arrays.asList(STREET_NUM.newCell(45), ZIP_CODE.newCell(123456))).isPresent());

    // Check that all the updates had an effect on the data
    Optional<Record<String>> recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Arrays.asList(STREET_NUM.newCell(45), ZIP_CODE.newCell(123456))));

    // check that the above updates did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // Update the record again, with an intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(abcAccessor.update(install(Collections.singleton(STREET_NUM.newCell(50)))).isPresent());
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(STREET_NUM.newCell(50))));

    // Update the record again, with a write intrinsic UpdateOperation
    resetDatasetEntity();
    assertTrue(abcAccessor.update(write(STREET_NUM.name(), 60)).isPresent());
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(STREET_NUM.newCell(60))));

    resetDatasetEntity();
    abcAccessor.update(UpdateOperation.write(STREET_NUM).resultOf(ZIP_CODE.valueOr(555)));
    verify(datasetEntity, times(0)).get(any());
    abcAccessor.update(UpdateOperation.write(ZIP_CODE).intResultOf(ZIP_CODE.intValueOr(-999)));
    verify(datasetEntity, times(0)).get(any());
    abcAccessor.update(UpdateOperation.write(ID).longResultOf(ID.longValueOr(-88888)));
    verify(datasetEntity, times(0)).get(any());
    abcAccessor.update(UpdateOperation.write(AVG).doubleResultOf(AVG.doubleValueOr(Double.NaN)));
    verify(datasetEntity, times(0)).get(any());
    abcAccessor.update(UpdateOperation.write(STREET_NAME).resultOf(STREET_NAME.valueOr("da street")));
    verify(datasetEntity, times(0)).get(any());
    abcAccessor.update(UpdateOperation.write(VALID).resultOf(VALID.valueOr(true)));
    verify(datasetEntity, times(0)).get(any());
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Arrays.asList(STREET_NUM.newCell(555), ZIP_CODE.newCell(-999), ID.newCell(-88888L), AVG.newCell(Double.NaN), STREET_NAME.newCell("da street"), VALID.newCell(true))));
  }

  @Test
  public void predicatedUpdateRecords() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    ReadWriteRecordAccessor<String> abcAccessor = spy(access.on("abc"));
    // record does not exist
    assertFalse(access.get("abc").isPresent());

    // update fails as the record doesn't exist
    assertFalse(abcAccessor.iff(r -> true).update(install(Collections.singleton(STREET_NUM.newCell(30)))).isPresent());

    // check that the above update did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // same as above, but with an intrinsic predicate
    resetDatasetEntity();
    assertFalse(abcAccessor.iff(AlwaysTrue.alwaysTrue()).update(install(Collections.singleton(STREET_NUM.newCell(30)))).isPresent());
    verify(datasetEntity, times(0)).get(any());

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now the record can be updated
    assertTrue(abcAccessor.iff(r -> true).update(install(Collections.singleton(STREET_NUM.newCell(30)))).isPresent());
    assertTrue(abcAccessor.iff(r -> true).update(install(Collections.singleton(STREET_NUM.newCell(40)))).isPresent());
    assertTrue(abcAccessor.iff(r -> true).update(install(Collections.singleton(ZIP_CODE.newCell(123456)))).isPresent());

    // Check that all the updates had an effect on the data
    Optional<Record<String>> recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(ZIP_CODE.newCell(123456))));

    // Try to update the record with a failing predicate
    assertFalse(abcAccessor.iff(r -> false).update(install(Collections.singleton(STREET_NUM.newCell(50)))).isPresent());

    // Check that all the update had no effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(ZIP_CODE.newCell(123456))));

    // check that the above updates did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // Update the record again, with an intrinsic UpdateOperation and Predicate
    resetDatasetEntity();
    assertTrue(abcAccessor.iff(AlwaysTrue.alwaysTrue()).update(install(Collections.singleton(STREET_NUM.newCell(50)))).isPresent());
    verify(datasetEntity, times(0)).get(any());

    // Check that the intrinsic update had an effect on the data
    recOpt = access.get("abc");
    assertTrue(recOpt.isPresent());
    assertTrue(cellsEqual(recOpt.get(), Collections.singleton(STREET_NUM.newCell(50))));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void updateWrongKeyTypeThrows() throws Exception {
    // don't use a genericized access on purpose
    DatasetWriterReader access = dataset.writerReader();

    expectedException.expect(Exception.class);
    access.update(1, r -> null);
  }

  @Test
  public void updateRecordWithMultipleCells() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    BoolCellDefinition boolCell = CellDefinition.defineBool("boolCell");
    CharCellDefinition charCell = CellDefinition.defineChar("charCell");
    IntCellDefinition intCell = CellDefinition.defineInt("intCell");
    LongCellDefinition longCell = CellDefinition.defineLong("longCell");
    DoubleCellDefinition doubleCell = CellDefinition.defineDouble("doubleCell");
    StringCellDefinition stringCell = CellDefinition.defineString("stringCell");
    CellDefinition<byte[]> bytesCell = CellDefinition.define("bytesCell", Type.BYTES);

    assertTrue(access.add("abc",
        boolCell.newCell(true),
        charCell.newCell('C'),
        intCell.newCell(-100),
        longCell.newCell(789L),
        doubleCell.newCell(6.7),
        stringCell.newCell("bill"),
        bytesCell.newCell(new byte[]{-1, 0, 1})
    ));

    assertTrue(access.update("abc", cells -> Arrays.asList(
        boolCell.newCell(false),
        charCell.newCell('D'),
        intCell.newCell(100),
        longCell.newCell(-789L),
        doubleCell.newCell(Double.NaN),
        stringCell.newCell("steve"),
        bytesCell.newCell(new byte[]{10, 0, -10})
    )));

    Optional<Record<String>> recordHolder = access.get("abc");
    assertTrue(recordHolder.isPresent());
    Record<String> record = recordHolder.get();

    assertFalse(record.get(boolCell).get());
    assertTrue(record.get(charCell).get() == 'D');
    assertTrue(record.get(intCell).get() == 100);
    assertTrue(record.get(longCell).get() == -789L);
    assertTrue(record.get(doubleCell).get().isNaN());
    assertEquals("steve", record.get(stringCell).get());
    assertArrayEquals(new byte[]{10, 0, -10}, record.get(bytesCell).get());
  }

  private static boolean cellsEqual(Iterable<Cell<?>> r1, Iterable<Cell<?>> r2) {
    List<Cell<?>> expectCells = new ArrayList<>();
    r1.forEach(expectCells::add);

    for (Cell<?> cell : r2) {
      if (!expectCells.remove(cell)) {
        return false;
      }
    }
    return expectCells.isEmpty();
  }
}
