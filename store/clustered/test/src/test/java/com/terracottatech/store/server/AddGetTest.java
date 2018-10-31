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

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class AddGetTest extends PassthroughTest {
  private final List<String> stripeNames;

  public AddGetTest(List<String> stripeNames) {
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
  public void addRecord() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    // Neither record exists
    assertFalse(access.get("abc").isPresent());
    assertFalse(access.get("def").isPresent());

    // Add a record - get true
    assertTrue(access.add("abc"));

    // Now one exists and the other does not
    assertTrue(access.get("abc").isPresent());
    assertFalse(access.get("def").isPresent());

    // Add the same record again - get false
    assertFalse(access.add("abc"));

    // There's still one record there, not the other
    assertTrue(access.get("abc").isPresent());
    assertFalse(access.get("def").isPresent());
  }

  @Test
  public void addRecordReturnExisting() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();
    // No record exists
    assertFalse(access.get("abc").isPresent());

    // Add a record - get empty optional
    assertThat(access.on("abc").add(CellDefinition.defineString("firstName").newCell("john")).isPresent(), is(false));

    // Now the record exists
    assertTrue(access.get("abc").isPresent());

    // Add a record again - get the previous one in the optional
    Optional<Record<String>> optRecord = access.on("abc").add(CellDefinition.defineString("lastName").newCell("doe"));
    assertThat(optRecord.isPresent(), is(true));
    assertThat(optRecord.get().getKey(), is("abc"));
    assertThat(optRecord.get().get(CellDefinition.defineString("firstName")).get(), is("john"));

    // The old record is still present
    optRecord = access.get("abc");
    assertThat(optRecord.isPresent(), is(true));
    assertThat(optRecord.get().getKey(), is("abc"));
    assertThat(optRecord.get().get(CellDefinition.defineString("firstName")).get(), is("john"));
  }

  @Test
  public void addRecordWithOneStringCell() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    StringCellDefinition cellDef = CellDefinition.defineString("cell");
    assertTrue(access.add("abc", cellDef.newCell("value")));

    Optional<Record<String>> recordHolder = access.get("abc");
    assertTrue(recordHolder.isPresent());

    Record<String> record = recordHolder.get();
    Optional<String> valueHolder = record.get(cellDef);
    assertTrue(valueHolder.isPresent());

    String value = valueHolder.get();
    assertEquals("value", value);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void noTricksyGenericsAvoidingAllowed() throws Exception {
    try {
      DatasetWriterReader<String> access = dataset.writerReader();

      CellDefinition cellDef = CellDefinition.defineString("cell");
      assertTrue(access.add("abc", cellDef.newCell(123)));

      fail("Expected an exception");
    } catch (StoreRuntimeException e) {
      // Test success
      assertThat(e.getCause().getCause(), Matchers.instanceOf(ClassCastException.class));
    }
  }

  @Test
  public void addRecordWithMultipleCells() throws Exception {
    DatasetWriterReader<String> access = dataset.writerReader();

    BoolCellDefinition boolCell = CellDefinition.defineBool("boolCell");
    CharCellDefinition charCell = CellDefinition.defineChar("charCell");
    IntCellDefinition intCell = CellDefinition.defineInt("intCell");
    LongCellDefinition longCell = CellDefinition.defineLong("longCell");
    DoubleCellDefinition doubleCell = CellDefinition.defineDouble("doubleCell");
    StringCellDefinition stringCell = CellDefinition.defineString("stringCell");
    BytesCellDefinition bytesCell = CellDefinition.defineBytes("bytesCell");

    assertTrue(access.add("abc",
        boolCell.newCell(true),
        charCell.newCell('C'),
        intCell.newCell(-100),
        longCell.newCell(789L),
        doubleCell.newCell(6.7),
        stringCell.newCell("bill"),
        bytesCell.newCell(new byte[]{-1, 0, 1})
    ));

    Optional<Record<String>> recordHolder = access.get("abc");
    assertTrue(recordHolder.isPresent());

    Record<String> record = recordHolder.get();

    assertTrue(record.get(boolCell).get());
    assertTrue(record.get(charCell).get() == 'C');
    assertTrue(record.get(intCell).get() == -100);
    assertTrue(record.get(longCell).get() == 789L);
    assertTrue(record.get(doubleCell).get() == 6.7);
    assertEquals("bill", record.get(stringCell).get());
    assertArrayEquals(new byte[]{-1, 0, 1}, record.get(bytesCell).get());
  }
}
