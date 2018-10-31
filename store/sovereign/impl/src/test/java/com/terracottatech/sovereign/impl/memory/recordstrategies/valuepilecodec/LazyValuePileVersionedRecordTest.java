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

package com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec;

import org.junit.Before;
import org.junit.Test;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

/**
 * Provides basic tests for {@link LazyValuePileVersionedRecord}.
 */
public class LazyValuePileVersionedRecordTest {

  private ValuePileRecordBufferStrategy<Integer> strat;
  private SovereignDataSetConfig<Integer, SystemTimeReference> config;

  @Before
  public void setup() {
    config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class)
        .timeReferenceGenerator(new SystemTimeReference.Generator())
        .freeze();
    strat = new ValuePileRecordBufferStrategy<>(config, new DatasetSchemaImpl(), true);
  }

  @Test
  public void testToString() throws Exception {
    SovereignPersistentRecord<Integer> record = getRecord(10,
        Cell.cell("foo", "all for one"),
        Cell.cell("bar", 10),
        Cell.cell("bingo", false),
        Cell.cell("badminton", 100L));
    SovereignPersistentRecord<Integer> lazyRecord = toLazyRecord(record);

    String originalRecordString = record.toString();
    String lazyRecordString = lazyRecord.toString();

    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='foo' type='Type<String>'] value='all for one']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='bar' type='Type<Integer>'] value='10']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='bingo' type='Type<Boolean>'] value='false']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='badminton' type='Type<Long>'] value='100']"));
    assertThat(originalRecordString, containsString("key=10,"));
    assertThat(originalRecordString, containsString("msn=1000,"));

    assertThat(lazyRecordString, containsString("Cell[definition=CellDefinition[name='foo' type='Type<String>'] value='all for one']"));
    assertThat(lazyRecordString, containsString("Cell[definition=CellDefinition[name='bar' type='Type<Integer>'] value='10']"));
    assertThat(lazyRecordString, containsString("Cell[definition=CellDefinition[name='bingo' type='Type<Boolean>'] value='false']"));
    assertThat(lazyRecordString, containsString("Cell[definition=CellDefinition[name='badminton' type='Type<Long>'] value='100']"));
    assertThat(lazyRecordString, containsString("key=10,"));
    assertThat(lazyRecordString, containsString("msn=1000,"));
  }

  @Test
  public void testToStringBoolean() throws Exception {
    SovereignPersistentRecord<Integer> record = getRecord(10,
        Cell.cell("foo", "all for one"),
        Cell.cell("bar", 10),
        Cell.cell("bingo", false),
        Cell.cell("badminton", 100L));
    SovereignPersistentRecord<Integer> lazyRecord = toLazyRecord(record);

    String originalRecordString = record.toString();
    String lazyRecordString = ((LazyValuePileVersionedRecord)lazyRecord).toString(false);

    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='foo' type='Type<String>'] value='all for one']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='bar' type='Type<Integer>'] value='10']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='bingo' type='Type<Boolean>'] value='false']"));
    assertThat(originalRecordString, containsString("Cell[definition=CellDefinition[name='badminton' type='Type<Long>'] value='100']"));
    assertThat(originalRecordString, containsString("key=10,"));
    assertThat(originalRecordString, containsString("msn=1000,"));

    String[] cellTargets = new String[record.size()];
    Arrays.fill(cellTargets, " definition=null value='null'");
    assertThat(lazyRecordString, stringContainsInOrder(Arrays.asList(cellTargets)));
    assertThat(lazyRecordString, not(containsString("CellDefinition")));
    assertThat(lazyRecordString, containsString("key=10,"));
    assertThat(lazyRecordString, containsString("msn=1000,"));
  }

  private SovereignPersistentRecord<Integer> getRecord(int key, Cell<?>... cells) {
    VersionedRecord<Integer> parent = new VersionedRecord<>();
    SingleRecord<Integer> rec = new SingleRecord<>(parent,
        key,
        config.getTimeReferenceGenerator().get(),
        1000L,
        cells);
    parent.elements().add(rec);
    return parent;
  }

  private SovereignPersistentRecord<Integer> toLazyRecord(SovereignPersistentRecord<Integer> parent) {
    return strat.fromByteBuffer(strat.toByteBuffer(parent));
  }
}