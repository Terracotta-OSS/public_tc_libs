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

import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class DeleteTest extends PassthroughTest {

  private final List<String> stripeNames;

  public DeleteTest(List<String> stripeNames) {
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

  @Test
  public void deleteRecords() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    // Neither record exists
    assertFalse(access.get("abc").isPresent());
    assertFalse(access.get("def").isPresent());

    // delete fails as the records don't exist
    assertFalse(access.delete("abc"));
    assertFalse(access.delete("def"));

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now one can be and the other cannot
    assertTrue(access.delete("abc"));
    assertFalse(access.delete("def"));

    // Add the same record again - get true
    assertTrue(access.add("abc"));

    // There's still one record there, not the other
    assertTrue(access.delete("abc"));
    assertFalse(access.delete("def"));
  }

  @Test
  public void deleteRecordsReturnRecord() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    // Neither record exists
    assertFalse(access.get("abc").isPresent());
    assertFalse(access.get("def").isPresent());

    // delete fails as the records don't exist
    assertFalse(access.on("abc").delete().isPresent());
    assertFalse(access.on("def").delete().isPresent());

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now one can be and the other cannot
    Optional<Record<String>> abc = access.on("abc").delete();
    assertTrue(abc.isPresent());
    assertEquals(abc.get().getKey(), "abc");
    assertFalse(access.on("def").delete().isPresent());

    // Add the same record again - get true
    assertTrue(access.add("abc"));

    // There's still one record there, not the other
    abc = access.on("abc").delete();
    assertTrue(abc.isPresent());
    assertEquals(abc.get().getKey(), "abc");
    assertFalse(access.on("def").delete().isPresent());
  }

  @Test
  public void predicatedDeleteRecordsReturnRecord() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    // No record exists
    assertFalse(access.get("abc").isPresent());

    // delete fails as the record doesn't exist
    assertFalse(access.on("abc").iff(r -> true).delete().isPresent());

    // check that the above update did use the get & CAS loop
    verify(datasetEntity, atLeast(1)).get(any());

    // Add a record - get true
    assertTrue(access.add("abc"));

    // get the record back for comparison
    Record<String> record = access.get("abc").get();

    // Now it can be deleted (with an intrinsic operation)
    resetDatasetEntity();
    Optional<Record<String>> abc = access.on("abc").iff(record.getEqualsPredicate()).delete();
    assertTrue(abc.isPresent());
    assertEquals(abc.get().getKey(), "abc");
    verify(datasetEntity, times(0)).get(any());

    // Add the same record again - get true
    assertTrue(access.add("abc"));

    // can't delete when the predicate returns false
    resetDatasetEntity();
    assertFalse(access.on("abc").iff(r -> false).delete().isPresent());
    verify(datasetEntity, atLeast(1)).get(any());

    // can delete when the predicate tests for equality against the deleted record (with an intrinsic operation)
    resetDatasetEntity();
    abc = access.on("abc").iff(record.getEqualsPredicate()).delete();
    assertTrue(abc.isPresent());
    assertEquals(abc.get().getKey(), "abc");
    verify(datasetEntity, times(0)).get(any());

    // Add the same record again - get true
    assertTrue(access.add("abc"));

    // delete without an intrinsic operation
    resetDatasetEntity();
    abc = access.on("abc").iff(record::equals).delete();
    assertTrue(abc.isPresent());
    assertEquals(abc.get().getKey(), "abc");
    verify(datasetEntity, atLeast(1)).get(any());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void deleteWrongKeyTypeThrows() throws Exception {
    // don't use a genericized access on purpose
    DatasetWriterReader access = dataset.writerReader();

    expectedException.expect(Exception.class);
    access.delete(1);
  }

  @Test
  public void deleteRecordWithMultipleCells() throws Exception {
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

    assertTrue(access.delete("abc"));

    Optional<Record<String>> recordHolder = access.get("abc");
    assertFalse(recordHolder.isPresent());
  }
}
