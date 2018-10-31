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
package com.terracottatech.store.systemtest;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import org.junit.Test;

import java.net.URI;
import java.util.Optional;
import java.util.function.Predicate;

import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.CHAR;
import static com.terracottatech.store.Type.DOUBLE;
import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.UpdateOperation.write;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BasicOperationsIT extends BaseClusterWithOffheapAndFRSTest {
  private static final String BOOL_DATASET = "boolData";
  private static final String CHAR_DATASET = "charData";
  private static final String INT_DATASET = "intData";
  private static final String LONG_DATASET = "longData";
  private static final String DOUBLE_DATASET = "doubleData";
  private static final String STRING_DATASET = "stringData";
  private static final String LONG_DATASET_2 = "longData2";
  private static final String LIFECYCLE_DATASET = "lifecycle";

  private static final BoolCellDefinition BOOL_CELL = CellDefinition.defineBool("boolCell");
  private static final CharCellDefinition CHAR_CELL = CellDefinition.defineChar("charCell");
  private static final IntCellDefinition INT_CELL = CellDefinition.defineInt("intCell");
  private static final LongCellDefinition LONG_CELL = CellDefinition.defineLong("longCell");
  private static final DoubleCellDefinition DOUBLE_CELL = CellDefinition.defineDouble("doubleCell");
  private static final StringCellDefinition STRING_CELL = CellDefinition.defineString("stringCell");
  private static final CellDefinition<byte[]> BYTES_CELL = CellDefinition.define("bytesCell", Type.BYTES);
  private static final StringCellDefinition MISSING_CELL = CellDefinition.defineString("missingCell");

  @Test
  public void basicCRUD() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
      // Simple add and get
      assertThat(datasetManager.newDataset(LONG_DATASET, LONG, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 1L, true, 'a', Integer.MAX_VALUE, Long.MAX_VALUE, Double.MIN_NORMAL, "value1", new byte[] { -1 });
        assertNoRecord(writerReader, 2L);
      }

      // Check that the record is still there with a new Dataset
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetReader<Long> reader = dataset.reader();
        getAndAssertRecord(reader, 1L, true, 'a', Integer.MAX_VALUE, Long.MAX_VALUE, Double.MIN_NORMAL, "value1", new byte[] { -1 });
        assertNoRecord(reader, 2L);
      }

      // Add another record - check the first one is still good
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "", new byte[] {});
        getAndAssertRecord(writerReader, 1L, true, 'a', Integer.MAX_VALUE, Long.MAX_VALUE, Double.MIN_NORMAL, "value1", new byte[] { -1 });
      }

      // No impact on other datasets
      assertThat(datasetManager.newDataset(LONG_DATASET_2, LONG, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET_2, LONG)) {
        DatasetReader<Long> reader = dataset.reader();
        assertNoRecord(reader, 1L);
        assertNoRecord(reader, 2L);
      }

      // Delete
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        writerReader.delete(1L);
        assertNoRecord(writerReader, 1L);
        getAndAssertRecord(writerReader, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "", new byte[] {});
      }

      // Update
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        boolean updated1 = writerReader.update(1L, write(STRING_CELL.newCell("value2")));
        boolean updated2 = writerReader.update(2L, write(STRING_CELL.newCell("value2")));
        assertFalse(updated1);
        assertTrue(updated2);

        assertNoRecord(writerReader, 1L);
        getAndAssertRecord(writerReader, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value2", new byte[] {});
      }

      // Conditional update - condition returns false
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        Optional<Tuple<Record<Long>, Record<Long>>> update1 = writerReader.on(1L)
            .iff(recordHasStringCellValue("value"))
            .update(write(STRING_CELL.newCell("value3")));
        Optional<Tuple<Record<Long>, Record<Long>>> update2 = writerReader.on(2L)
            .iff(recordHasStringCellValue("value"))
            .update(write(STRING_CELL.newCell("value3")));
        assertFalse(update1.isPresent());
        assertFalse(update2.isPresent());

        assertNoRecord(writerReader, 1L);
        getAndAssertRecord(writerReader, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value2", new byte[] {});
      }

      // Conditional update - condition returns true
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        Optional<Tuple<Record<Long>, Record<Long>>> update1 = writerReader.on(1L)
            .iff(recordHasStringCellValue("value2"))
            .update(write(STRING_CELL.newCell("value3")));
        Optional<Tuple<Record<Long>, Record<Long>>> update2 = writerReader.on(2L)
            .iff(recordHasStringCellValue("value2"))
            .update(write(STRING_CELL.newCell("value3")));
        assertFalse(update1.isPresent());
        assertTrue(update2.isPresent());

        Tuple<Record<Long>, Record<Long>> tuple2 = update2.get();
        Record<Long> before = tuple2.getFirst();
        Record<Long> after = tuple2.getSecond();

        assertRecord(before, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value2", new byte[] {});
        assertRecord(after, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value3", new byte[] {});

        assertNoRecord(writerReader, 1L);
        getAndAssertRecord(writerReader, 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value3", new byte[] {});
      }

      // Conditional delete
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        Optional<Record<Long>> delete1 = writerReader.on(1L).iff(recordHasStringCellValue("value3")).delete();
        Optional<Record<Long>> delete2 = writerReader.on(2L).iff(recordHasStringCellValue("value3")).delete();
        assertFalse(delete1.isPresent());
        assertTrue(delete2.isPresent());

        assertRecord(delete2.get(), 2L, false, (char) 0, Integer.MIN_VALUE, Long.MIN_VALUE, Double.MIN_VALUE, "value3", new byte[] {});

        assertNoRecord(writerReader, 1L);
        assertNoRecord(writerReader, 2L);
      }

      // Recreate the records ready for more testing
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 1L, true, (char) 1000, -1, -1L, -1.0, "value4", new byte[] { -1, -2, -3 });
        addAndAssert(writerReader, 2L, false, (char) 3000, -3, -3L, -3.0, "value5", new byte[] { -7, -8, -9 });
      }

      // Mapping update
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        Optional<Optional<String>> update1 = writerReader.on(1L)
            .update(write(BOOL_CELL.newCell(true)), (before, after) -> after.get(STRING_CELL));
        Optional<Optional<String>> update2 = writerReader.on(2L)
            .update(write(BOOL_CELL.newCell(true)), (before, after) -> after.get(MISSING_CELL));
        update1.get().get().equals("value4");
        assertTrue(update2.isPresent());
        assertFalse(update2.get().isPresent());

        getAndAssertRecord(writerReader, 1L, true, (char) 1000, -1, -1L, -1.0, "value4", new byte[] { -1, -2, -3 });
        getAndAssertRecord(writerReader, 2L, true, (char) 3000, -3, -3L, -3.0, "value5", new byte[] { -7, -8, -9 });
      }

      // Mapping delete
      try (Dataset<Long> dataset = datasetManager.getDataset(LONG_DATASET, LONG)) {
        DatasetWriterReader<Long> writerReader = dataset.writerReader();
        Optional<Optional<String>> delete1 = writerReader.on(1L).delete(r -> r.get(STRING_CELL));
        Optional<Optional<String>> delete2a = writerReader.on(2L)
            .iff(r -> r.get(MISSING_CELL).isPresent())
            .delete(r -> r.get(STRING_CELL));
        assertEquals("value4", delete1.get().get());
        assertFalse(delete2a.isPresent());

        assertNoRecord(writerReader, 1L);
        getAndAssertRecord(writerReader, 2L, true, (char) 3000, -3, -3L, -3.0, "value5", new byte[] { -7, -8, -9 });

        Optional<String> delete2b = writerReader.on(2L)
            .iff(r -> r.get(STRING_CELL).isPresent())
            .delete(r -> r.get(STRING_CELL).get());
        assertEquals("value5", delete2b.get());

        assertNoRecord(writerReader, 1L);
        assertNoRecord(writerReader, 2L);
      }
    }
  }

  private Predicate<? super Record<Long>> recordHasStringCellValue(String expectedValue) {
    return r -> r.get(STRING_CELL).map(s -> s.equals(expectedValue)).orElse(false);
  }

  @Test
  public void variedDatasetKeyTypes() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
      assertThat(datasetManager.newDataset(BOOL_DATASET, BOOL, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Boolean> dataset = datasetManager.getDataset(BOOL_DATASET, BOOL)) {
        DatasetWriterReader<Boolean> writerReader = dataset.writerReader();
        addAndAssert(writerReader, true, false, 'A', 1, 1L, 1.1, "AAA", new byte[] { 1, 2, 3 });
        assertNoRecord(writerReader, false);
      }

      assertThat(datasetManager.newDataset(CHAR_DATASET, CHAR, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Character> dataset = datasetManager.getDataset(CHAR_DATASET, CHAR)) {
        DatasetWriterReader<Character> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 'B', false, 'A', 1, 1L, 1.1, "AAA", new byte[] { 1, 2, 3 });
        assertNoRecord(writerReader, 'A');
        assertNoRecord(writerReader, 'C');
      }

      assertThat(datasetManager.newDataset(INT_DATASET, INT, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Integer> dataset = datasetManager.getDataset(INT_DATASET, INT)) {
        DatasetWriterReader<Integer> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 2, false, 'A', 1, 1L, 1.1, "AAA", new byte[] { 1, 2, 3 });
        assertNoRecord(writerReader, 1);
        assertNoRecord(writerReader, 3);
      }

      assertThat(datasetManager.newDataset(DOUBLE_DATASET, DOUBLE, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<Double> dataset = datasetManager.getDataset(DOUBLE_DATASET, DOUBLE)) {
        DatasetWriterReader<Double> writerReader = dataset.writerReader();
        addAndAssert(writerReader, 1.2, false, 'A', 1, 1L, 1.1, "AAA", new byte[] { 1, 2, 3 });
        assertNoRecord(writerReader, 1.1);
        assertNoRecord(writerReader, 1.3);
      }

      assertThat(datasetManager.newDataset(STRING_DATASET, STRING, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<String> dataset = datasetManager.getDataset(STRING_DATASET, STRING)) {
        DatasetWriterReader<String> writerReader = dataset.writerReader();
        addAndAssert(writerReader, "BBB", false, 'A', 1, 1L, 1.1, "AAA", new byte[] { 1, 2, 3 });
        assertNoRecord(writerReader, "AAA");
        assertNoRecord(writerReader, "CCC");
      }
    }
  }

  @Test
  public void lifecycle() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
      assertThat(datasetManager.newDataset(LIFECYCLE_DATASET, STRING, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<String> dataset = datasetManager.getDataset(LIFECYCLE_DATASET, STRING)) {
        DatasetWriterReader<String> writerReader = dataset.writerReader();
        writerReader.add("KEY");
      }

      datasetManager.destroyDataset(LIFECYCLE_DATASET);

      assertThat(datasetManager.newDataset(LIFECYCLE_DATASET, STRING, datasetManager.datasetConfiguration()
          .offheap("primary-server-resource")
          .build()), is(true));
      try (Dataset<String> dataset = datasetManager.getDataset(LIFECYCLE_DATASET, STRING)) {
        DatasetWriterReader<String> writerReader = dataset.writerReader();
        assertNoRecord(writerReader, "KEY");
      }
    }
  }

  private <K extends Comparable<K>> void addAndAssert(DatasetWriterReader<K> writerReader, K key, boolean boolValue, char charValue, int intValue, long longValue, double doubleValue, String stringValue, byte[] bytesValue) {
    addRecord(writerReader, key, boolValue, charValue, intValue, longValue, doubleValue, stringValue, bytesValue);
    getAndAssertRecord(writerReader, key, boolValue, charValue, intValue, longValue, doubleValue, stringValue, bytesValue);
  }

  private <K extends Comparable<K>> void addRecord(DatasetWriterReader<K> writerReader, K key, boolean boolValue, char charValue, int intValue, long longValue, double doubleValue, String stringValue, byte[] bytesValue) {
    boolean result = writerReader.add(key,
            BOOL_CELL.newCell(boolValue),
            CHAR_CELL.newCell(charValue),
            INT_CELL.newCell(intValue),
            LONG_CELL.newCell(longValue),
            DOUBLE_CELL.newCell(doubleValue),
            STRING_CELL.newCell(stringValue),
            BYTES_CELL.newCell(bytesValue)
    );

    assertTrue(result);
  }

  private <K extends Comparable<K>> void getAndAssertRecord(DatasetReader<K> reader, K key, boolean boolValue, char charValue, int intValue, long longValue, double doubleValue, String stringValue, byte[] bytesValue) {
    Record<K> record = reader.get(key).get();
    assertRecord(record, key, boolValue, charValue, intValue, longValue, doubleValue, stringValue, bytesValue);
  }

  private <K extends Comparable<K>> void assertRecord(Record<K> record, K key, boolean boolValue, char charValue, int intValue, long longValue, double doubleValue, String stringValue, byte[] bytesValue) {
    assertEquals(key, record.getKey());
    assertEquals(boolValue, record.get(BOOL_CELL).get());
    assertEquals(charValue, (char) record.get(CHAR_CELL).get());
    assertEquals(intValue, (int) record.get(INT_CELL).get());
    assertEquals(longValue, (long) record.get(LONG_CELL).get());
    assertTrue(doubleValue == record.get(DOUBLE_CELL).get());
    assertEquals(stringValue, record.get(STRING_CELL).get());
    assertArrayEquals(bytesValue, record.get(BYTES_CELL).get());
  }

  private <K extends Comparable<K>> void assertNoRecord(DatasetReader<K> reader, K key) {
    assertFalse(reader.get(key).isPresent());
  }
}
