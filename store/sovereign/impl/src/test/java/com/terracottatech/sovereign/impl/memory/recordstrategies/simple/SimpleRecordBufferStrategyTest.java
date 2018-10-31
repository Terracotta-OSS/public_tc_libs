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

package com.terracottatech.sovereign.impl.memory.recordstrategies.simple;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;

import com.terracottatech.store.definition.BoolCellDefinition;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by cschanck on 7/7/2015.
 */
public class SimpleRecordBufferStrategyTest {

  private SovereignDataSetConfig<Integer, SystemTimeReference> config;
  private SystemTimeReference constantTime;

  @Before
  public void before() {
    this.config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    this.constantTime = this.config.getTimeReferenceGenerator().get();
  }

  @Test
  public void testCellValues() throws IOException {
    testValue(Boolean.TRUE, Type.BOOL);
    testValue(Boolean.FALSE, Type.BOOL);
    testValue('a', Type.CHAR);
    testValue(100, Type.INT);
    testValue((long)1000, Type.LONG);
    testValue(1000.100d, Type.DOUBLE);
    testValue("Foo bar", Type.STRING);
    // bytes later
  }

  private void testValue(Object value, Type<?> type) throws IOException {
    final SimpleRecordBufferStrategy<Integer> bufferStrategy =
      new SimpleRecordBufferStrategy<>(config, null);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream oos = new DataOutputStream(baos)) {
      bufferStrategy.writeCellValue(oos, Cell.cell("foo", value));
      oos.flush();
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           DataInputStream ois = new DataInputStream(bais)) {
        Cell<?> got = bufferStrategy.readCellValue(ois);
        assertThat(got.definition().type(), is(type));
        assertThat(got.value(), is(value));
      }
    }

  }

  @Test
  public void testSingleRecordSystemTimeReference() throws IOException {
    SingleRecord<Integer> rec =
        new SingleRecord<>(null, 10, constantTime, 1000, Cell.cell("foo", "a"), Cell.cell("bar", 10));
    SingleRecord<Integer> next = roundTripSingleRecord(this.config, rec);
    assertThat(rec, is(next));
    assertTrue(rec.deepEquals(next));
  }

  @Test
  public void testSingleRecordNullTimeReference() throws Exception {
    final SovereignDataSetConfig<Integer, FixedTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, FixedTimeReference.class).freeze();
    final SingleRecord<Integer> rec =
        new SingleRecord<>(null, 10, config.getTimeReferenceGenerator().get(), 1000L, Cell.cell("foo", "a"), Cell.cell("bar", 10));
    final SingleRecord<Integer> next = roundTripSingleRecord(config, rec);
    assertThat(rec, is(next));
    assertTrue(rec.deepEquals(next));
  }

  @Test
  public void testSingleRecordTestTimeReference() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class)
            .timeReferenceGenerator(new TestTimeReference.Generator()).freeze();
    final SingleRecord<Integer> rec =
        new SingleRecord<>(null, 10, config.getTimeReferenceGenerator().get(), 1000L, Cell.cell("foo", "a"), Cell.cell("bar", 10));
    final SingleRecord<Integer> next = roundTripSingleRecord(config, rec);
    assertThat(rec, is(next));
    assertTrue(rec.deepEquals(next));
  }

  @SuppressWarnings("unchecked")
  private <K extends Comparable<K>>
  SingleRecord<K> roundTripSingleRecord(final SovereignDataSetConfig<K, ?> config, final SingleRecord<K> rec) throws
    IOException {
    final SimpleRecordBufferStrategy<K> bufferStrategy = new SimpleRecordBufferStrategy<>(config, null);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream oos = new DataOutputStream(baos)) {
      bufferStrategy.writeSingleRecord(oos, rec);
      oos.flush();
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           DataInputStream ois = new DataInputStream(bais)) {
        Object got = bufferStrategy.readSingleRecord(ois, null);
        assertEquals(got.getClass(), rec.getClass());
        return (SingleRecord<K>) got;
      }
    }
  }

  @Test
  public void testVersionedRecordWithOneVersion() throws IOException, ClassNotFoundException {
    VersionedRecord<Integer> v = new VersionedRecord<>();
    SingleRecord<Integer> rec =
        new SingleRecord<>(v, 10, constantTime, 1000, Cell.cell("foo", "a"), Cell.cell("bar", 10));
    v.elements().add(rec);
    VersionedRecord<Integer> got = read(write(v));
    assertEquals(v, got);
    assertTrue(v.deepEquals(got));
  }

  @Test
  public void testVersionedRecordWithFiveVersions() throws IOException, ClassNotFoundException {
    VersionedRecord<Integer> v = new VersionedRecord<>();
    for (int i = 0; i < 5; i++) {
      SingleRecord<Integer> rec =
          new SingleRecord<>(v, 10 + i, constantTime, 1000, Cell.cell("foo", "a"), Cell.cell("bar", 10 + i));
      v.elements().add(rec);
    }
    VersionedRecord<Integer> got = read(write(v));
    assertEquals(v, got);
    assertTrue(v.deepEquals(got));
    assertThat(v.versions().count(), is(5L));
    assertThat(v.get(CellDefinition.define("bar", Type.INT)).get(), is(10));
  }

  private ByteBuffer write(VersionedRecord<Integer> record) throws IOException {
    return new SimpleRecordBufferStrategy<>(config, null).toByteBuffer(record);
  }

  private VersionedRecord<Integer> read(ByteBuffer buf) {
    return new SimpleRecordBufferStrategy<>(config, null).fromByteBuffer(buf);
  }

  @Test
  public void testVersionedRecordReadKeyOnly() throws IOException, ClassNotFoundException {
    VersionedRecord<Integer> v = new VersionedRecord<>();
    for (int i = 0; i < 5; i++) {
      SingleRecord<Integer> rec =
          new SingleRecord<>(v, 10 + i, constantTime, 1000, Cell.cell("foo", "a"), Cell.cell("bar", 10 + i));
      v.elements().add(rec);
    }
    ByteBuffer wrote = write(v);
    Object k = new SimpleRecordBufferStrategy<>(config, null).readKey(wrote);
    assertNotNull(k);
    assertEquals(k.getClass(), Integer.class);
    assertThat(k, is(v.getKey()));
  }

  @Test
  public void testPackedDefNoSchema() throws Exception {
    BoolCellDefinition def1 = CellDefinition.defineBool("test1");
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SimpleRecordBufferStrategy.writePackedDefinition(null, dos, def1);
    dos.close();
    CellDefinition<?> t1 = SimpleRecordBufferStrategy.readPackedDefinition(null,
                                                                        new DataInputStream(new ByteArrayInputStream(
                                                                          baos.toByteArray())));
    assertThat(def1.name(), is(t1.name()));
    assertThat(def1.type(), is(t1.type()));
  }

  @Test
  public void testPackedDefSingle() throws Exception {
    DatasetSchemaImpl schema = new DatasetSchemaImpl();
    BoolCellDefinition def1 = CellDefinition.defineBool("test1");
    schema.idFor(def1);
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SimpleRecordBufferStrategy.writePackedDefinition(schema, dos, def1);
    dos.close();
    assertThat(baos.toByteArray().length, is(1));
    CellDefinition<?> t1 = SimpleRecordBufferStrategy.readPackedDefinition(schema,
                                                                        new DataInputStream(new ByteArrayInputStream(
                                                                          baos.toByteArray())));
    assertThat(def1.name(), is(t1.name()));
    assertThat(def1.type(), is(t1.type()));
  }

  @Test
  public void testPackedDefMoreThan63() throws Exception {
    DatasetSchemaImpl schema = new DatasetSchemaImpl();
    BoolCellDefinition def1 = CellDefinition.defineBool("test1");
    for(int i=0;i<100;i++) {
      schema.idFor(CellDefinition.defineBool("p-"+i));
    }
    schema.idFor(def1);
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SimpleRecordBufferStrategy.writePackedDefinition(schema, dos, def1);
    dos.close();
    assertThat(baos.toByteArray().length, is(3));
    CellDefinition<?> t1 = SimpleRecordBufferStrategy.readPackedDefinition(schema,
                                                                        new DataInputStream(new ByteArrayInputStream(
                                                                          baos.toByteArray())));
    assertThat(def1.name(), is(t1.name()));
    assertThat(def1.type(), is(t1.type()));
  }

  @Test
  public void testPackedDefMoreThanShortMax() throws Exception {
    DatasetSchemaImpl schema = new DatasetSchemaImpl();
    BoolCellDefinition def1 = CellDefinition.defineBool("test1");
    for(int i=0;i<Short.MAX_VALUE;i++) {
      schema.idFor(CellDefinition.defineBool("p-"+i));
    }
    schema.idFor(def1);
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SimpleRecordBufferStrategy.writePackedDefinition(schema, dos, def1);
    dos.close();
    assertThat(baos.toByteArray().length, is(9));
    CellDefinition<?> t1 = SimpleRecordBufferStrategy.readPackedDefinition(schema,
                                                                        new DataInputStream(new ByteArrayInputStream(
                                                                          baos.toByteArray())));
    assertThat(def1.name(), is(t1.name()));
    assertThat(def1.type(), is(t1.type()));
  }

  private static final class TestTimeReference implements TimeReference<TestTimeReference>, Serializable {
    private static final long serialVersionUID = 8079780365675024607L;
    private static final Comparator<TestTimeReference> COMPARATOR = Comparator.comparingLong(t -> t.timeReference);

    private final long timeReference;

    private TestTimeReference(final long timeReference) {
      this.timeReference = timeReference;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return COMPARATOR.compare(this, (TestTimeReference) t);
    }

    public static final class Generator implements TimeReferenceGenerator<TestTimeReference> {
      private static final long serialVersionUID = 7714251124117941854L;

      private final AtomicLong clock = new AtomicLong(0L);

      @Override
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }

      @Override
      public TestTimeReference get() {
        return new TestTimeReference(clock.incrementAndGet());
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }
    }
  }
}
