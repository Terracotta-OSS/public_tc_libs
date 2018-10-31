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
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class ReadTest extends PassthroughTest {
  private final List<String> stripeNames;

  public ReadTest(List<String> stripeNames) {
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
  public void readAndMapRecordOnReadWriteAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    ReadWriteRecordAccessor<String> abc = writerReader.on("abc");

    assertReadAndMapRecord(writerReader, abc);
  }

  @Test
  public void readAndMapRecordOnReadOnlyAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    ReadRecordAccessor<String> abc = dataset.reader().on("abc");

    assertReadAndMapRecord(writerReader, abc);
  }

  private void assertReadAndMapRecord(DatasetWriterReader<String> writerReader, ReadRecordAccessor<String> abc) {
    // No record exists
    assertFalse(writerReader.get("abc").isPresent());

    // Read and map non-existent record - get empty Optional
    assertFalse(abc.read(r -> "Record " + r.getKey()).isPresent());

    // Add a record - get true
    assertTrue(writerReader.add("abc"));

    // Read and map record - get a String
    assertEquals(abc.read(r -> "Record " + r.getKey()).get(), "Record abc");
  }

  @Test
  public void conditionallyReadRecordOnReadOnlyAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    ReadWriteRecordAccessor<String> abc = writerReader.on("abc");

    assertConditionallyReadRecord(datasetEntity, writerReader, abc);
  }

  @Test
  public void conditionallyReadRecordOnReadWriteAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    DatasetReader<String> reader = dataset.reader();
    ReadRecordAccessor<String> abc = reader.on("abc");

    assertConditionallyReadRecord(datasetEntity, writerReader, abc);
  }

  private void assertConditionallyReadRecord(DatasetEntity<String> datasetEntity, DatasetWriterReader<String> writerReader, ReadRecordAccessor<String> abc) {
    // No record exists
    assertFalse(writerReader.get("abc").isPresent());

    // Conditionally read non-existent record with false predicate - returns empty optional
    assertFalse(abc.iff(r -> false).read().isPresent());

    // Conditionally read non-existent record with true predicate - returns empty optional
    assertFalse(abc.iff(r -> true).read().isPresent());

    // Conditionally read record with intrinsic false predicate - returns empty optional
    resetDatasetEntity();
    assertFalse(abc.iff(defineString("does-not-exist").value().is("random blah")).read().isPresent());
    verify(datasetEntity, atLeast(1)).get(any(), notNull()); // check that the above update did ship the intrinsic

    // Conditionally read record with intrinsic true predicate - returns empty optional
    resetDatasetEntity();
    assertFalse(abc.iff(AlwaysTrue.alwaysTrue()).read().isPresent());
    verify(datasetEntity, atLeast(1)).get(any(), notNull());

    // Add a record - get true
    assertTrue(writerReader.add("abc", Cell.cell("firstName", "joe"), Cell.cell("lastName", "doe")));

    // Conditionally read record with false predicate - returns empty optional
    assertFalse(abc.iff(r -> false).read().isPresent());

    // Conditionally read record with true predicate - get record
    assertEquals(abc.iff(r -> true).read().get().getKey(), "abc");

//    // Conditionally read record with intrinsic false predicate - returns empty optional
//    assertFalse(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("does-not-exist", "random blah"))).read().isPresent());

    // Conditionally read record with intrinsic true predicate - get record
    resetDatasetEntity();
    assertEquals(abc.iff(AlwaysTrue.alwaysTrue()).read().get().getKey(), "abc");
    verify(datasetEntity, atLeast(1)).get(any(), notNull());

//    // intrinsic booleans - AND
//    reset(datasetEntity);
//    assertEquals(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "joe")).and(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "doe")))).read().get().getKey(), "abc");
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    reset(datasetEntity);
//    assertFalse(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "joe")).and(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "smith")))).read().isPresent());
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    reset(datasetEntity);
//    assertFalse(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "adam")).and(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "doe")))).read().isPresent());
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    reset(datasetEntity);
//    assertFalse(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "adam")).and(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "doe")))).read().isPresent());
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    // intrinsic booleans - OR
//    reset(datasetEntity);
//    assertEquals(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "adam")).or(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "doe")))).read().get().getKey(), "abc");
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    reset(datasetEntity);
//    assertFalse(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "adam")).or(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "smith")))).read().isPresent());
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
//
//    // intrinsic booleans - AND & OR
//    reset(datasetEntity);
//    assertEquals(abc.iff(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "adam")).or(Functions.cellValueEqualsPredicate(Cell.cell("lastName", "doe"))).and(Functions.cellValueEqualsPredicate(Cell.cell("firstName", "joe")))).read().get().getKey(), "abc");
//    verify(datasetEntity, atLeast(1)).get(any(), notNull(Predicate.class));
  }

  @Test
  public void conditionallyReadAndMapRecordOnReadOnlyAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    ReadRecordAccessor<String> abc = dataset.reader().on("abc");

    assertConditionallyReadAndMapRecord(writerReader, abc);
  }

  @Test
  public void conditionallyReadAndMapRecordOnReadWriteAccess() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    ReadWriteRecordAccessor<String> abc = writerReader.on("abc");

    assertConditionallyReadAndMapRecord(writerReader, abc);
  }

  private void assertConditionallyReadAndMapRecord(DatasetWriterReader<String> writerReader, ReadRecordAccessor<String> abc) {
    // No record exists
    assertFalse(writerReader.get("abc").isPresent());

    // Conditionally read and map non-existent record with false predicate - get empty Optional
    assertFalse(abc.iff(r -> false).read(r -> "Record " + r.getKey()).isPresent());

    // Conditionally read and map non-existent record with true predicate - get empty Optional
    assertFalse(abc.iff(r -> true).read(r -> "Record " + r.getKey()).isPresent());

    // Add a record - get true
    assertTrue(writerReader.add("abc"));

    // Conditionally read and map record with false predicate - get empty Optional
    assertFalse(abc.iff(r -> false).read(r -> "Record " + r.getKey()).isPresent());

    // Conditionally read and map record with true predicate - get a String
    assertEquals(abc.iff(r -> true).read(r -> "Record " + r.getKey()).get(), "Record abc");
  }
}
