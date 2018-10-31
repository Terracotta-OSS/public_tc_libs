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

package com.terracottatech.sovereign.impl;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.exceptions.MalformedCellException;
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.alterRecord;
import static com.terracottatech.sovereign.testsupport.RecordFunctions.compute;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;

public class SovereignDatasetImplTest {

  private static final boolean NOISY = false;

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> countCell = CellDefinition.define("count", Type.INT);

  private SovereignDataset<String> dataset;

  @After
  public void tearDown() throws Exception {
    if (this.dataset != null) {
      this.dataset.getStorage().destroyDataSet(dataset.getUUID());
      this.dataset = null;
    }
  }

  @Test
  public void testGetNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.get(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .get(Integer.MAX_VALUE);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testTryGetNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryGet(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryGetMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .tryGet(Integer.MAX_VALUE);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Ignore("get/tryGet locks only when under load")
  @Test
  public void testTryGetWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryGet(key), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }

  @Test
  public void testDelete2ArgNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.delete(IMMEDIATE, (String)null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDelete2ArgMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .delete(IMMEDIATE, Integer.MAX_VALUE);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testDelete2ArgMissingKey() throws Exception {
    this.setDataset();
    assertNull(this.dataset.delete(IMMEDIATE, "non-existent-key"));
  }

  @Test
  public void testTryDelete2ArgNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryDelete(IMMEDIATE, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryDelete2ArgMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .tryDelete(IMMEDIATE, Integer.MAX_VALUE);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testTryDelete2ArgMissingKey() throws Exception {
    this.setDataset();
    assertNull(this.dataset.tryDelete(IMMEDIATE, "non-existent-key"));
  }

  @Test
  public void testTryDelete2ArgWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryDelete(IMMEDIATE, key), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }

  @Test
  public void testDelete3ArgNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.delete(IMMEDIATE, null, r -> true);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testDelete3ArgNullPredicate() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.delete(IMMEDIATE, "existing", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDelete3ArgMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .delete(IMMEDIATE, Integer.MAX_VALUE, r -> true);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testDelete3ArgMissingKey() throws Exception {
    this.setDataset();
    assertNull(this.dataset.delete(IMMEDIATE, "non-existent-key", r -> true));
  }

  @Test
  public void testDelete3ArgFalse() throws Exception {
    this.setDataset();
    assertNull(this.dataset.delete(IMMEDIATE, "existing", r -> false));
  }

  @Test
  public void testTryDelete3ArgNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryDelete(IMMEDIATE, null, r -> true);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testTryDelete3ArgNullPredicate() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryDelete(IMMEDIATE, "existing", null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryDelete3ArgMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .tryDelete(IMMEDIATE, Integer.MAX_VALUE,  r -> true);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testTryDelete3ArgMissingKey() throws Exception {
    this.setDataset();
    assertNull(this.dataset.tryDelete(IMMEDIATE, "non-existent-key", r -> true));
  }

  @Test
  public void testTryDelete3ArgFalse() throws Exception {
    this.setDataset();
    assertNull(this.dataset.tryDelete(IMMEDIATE, "existing", r -> false));
  }

  @Test
  public void testTryDelete3ArgWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryDelete(IMMEDIATE, key, r -> true), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }

  @Test
  public void testAddNullKey() throws Exception {
    this.setDataset();
    assertNotNull(dataset);
    assertThrows(() -> dataset.add(IMMEDIATE, null, nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE)), NullPointerException.class);
    assertThrows(() -> dataset.add(IMMEDIATE, null, Arrays.asList(nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE))), NullPointerException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAddMismatchedKey() throws Exception {
    this.setDataset();
    assertThrows(() -> ((SovereignDataset<Integer>) (Object) dataset)         // unchecked -- intentionally cross typed
        .add(IMMEDIATE, Integer.MAX_VALUE, nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE)), ClassCastException.class);
    assertThrows(() -> ((SovereignDataset<Integer>) (Object) dataset)         // unchecked -- intentionally cross typed
        .add(IMMEDIATE, Integer.MAX_VALUE, CellSet.of(nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE))), ClassCastException.class);
  }

  /**
   * This test ensures that a {@code Cell} with a null value can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testAddNullValueCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", nameCell.newCell("foo"), getNullValueCell()), MalformedCellException.class);
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), (getNullValueCell()))), MalformedCellException.class);
  }

  /**
   * This test ensures that a {@code Cell} with a null definition can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testAddNullDefinitionCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", nameCell.newCell("foo"), getNullDefinitionCell()), MalformedCellException.class);
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), getNullDefinitionCell())), MalformedCellException.class);
  }

  /**
   * This test ensures that a {@code Cell} with mistyped value can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testAddMistypedValueCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", nameCell.newCell("foo"), getMistypedCell()), MalformedCellException.class);
    assertThrows(() -> dataset.add(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), getMistypedCell())), MalformedCellException.class);
  }

  @Test
  public void testTryAddNullKey() throws Exception {
    this.setDataset();
    assertNotNull(dataset);
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, null, nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE)), RecordLockedException.class, NullPointerException.class);
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, null, Arrays.asList(nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE))), RecordLockedException.class, NullPointerException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryAddMismatchedKey() throws Exception {
    this.setDataset();
    assertThrows(() -> ((SovereignDataset<Integer>) (Object) dataset)         // unchecked -- intentionally cross typed
        .tryAdd(IMMEDIATE, Integer.MAX_VALUE, nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE)), RecordLockedException.class, ClassCastException.class);
    assertThrows(() -> ((SovereignDataset<Integer>) (Object) dataset)         // unchecked -- intentionally cross typed
        .tryAdd(IMMEDIATE, Integer.MAX_VALUE, CellSet.of(nameCell.newCell("foo"), countCell.newCell(Integer.MAX_VALUE))), RecordLockedException.class, ClassCastException.class);
  }

  /**
   * This test ensures that a {@code Cell} with a null value can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testTryAddNullValueCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", nameCell.newCell("foo"), getNullValueCell()), RecordLockedException.class, MalformedCellException.class);
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), (getNullValueCell()))), RecordLockedException.class, MalformedCellException.class);
  }

  /**
   * This test ensures that a {@code Cell} with a null definition can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testTryAddNullDefinitionCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", nameCell.newCell("foo"), getNullDefinitionCell()), RecordLockedException.class, MalformedCellException.class);
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), getNullDefinitionCell())), RecordLockedException.class, MalformedCellException.class);
  }

  /**
   * This test ensures that a {@code Cell} with mistyped value can not be added to
   * a SovereignDataset.
   */
  @Test
  public void testTryAddMistypedValueCell() throws Exception {
    this.setDataset();
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", nameCell.newCell("foo"), getMistypedCell()), RecordLockedException.class, MalformedCellException.class);
    assertThrows(() -> dataset.tryAdd(IMMEDIATE, "recordName", Arrays.asList(nameCell.newCell("foo"), getMistypedCell())), RecordLockedException.class, MalformedCellException.class);
  }

  @Test
  public void testTryAddWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryAdd(IMMEDIATE, key), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3Arg() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");
    dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> r);
    Record<String> newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    dataset.applyMutation(IMMEDIATE, "existing", r -> true, alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))));
    newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @Test
  public void testApplyMutation3ArgKeyedNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.applyMutation(IMMEDIATE, null, r -> true, r -> r);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyMutation3ArgKeyedMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .applyMutation(IMMEDIATE, Integer.MAX_VALUE, r -> true, r -> r);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgKeyedNullValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullValueCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgKeyedNullDefinitionCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullDefinitionCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgKeyedMistypedValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getMistypedCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation3Arg() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");
    dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> r);
    Record<String> newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))));
    newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @Test
  public void testTryApplyMutation3ArgKeyedNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryApplyMutation(IMMEDIATE, null, r -> true, r -> r);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryApplyMutation3ArgKeyedMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .tryApplyMutation(IMMEDIATE, Integer.MAX_VALUE, r -> true, r -> r);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation3ArgKeyedNullValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, (Record<String> r) -> singleton(getNullValueCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation3ArgKeyedNullDefinitionCell() throws Exception {
    this.setDataset();
    try {
      dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullDefinitionCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation3ArgKeyedMistypedValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getMistypedCell()));
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation3ArgWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryApplyMutation(IMMEDIATE, key, r -> true, r -> r), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation4Arg() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");
    BiFunction<Record<String>, Record<String>, Record<String>> biFunction = (o, n) -> n;
    Optional<Record<String>> result = dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> r, biFunction);
    Record<String> newRecord = dataset.get("existing");
    assertThat(result.get(), is(equalTo(newRecord)));
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    result = dataset.applyMutation(IMMEDIATE, "existing",
                                   r -> true,
                                   alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))), biFunction);
    newRecord = dataset.get("existing");
    assertThat(result.get(), is(equalTo(newRecord)));
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @Test
  public void testApplyMutation4ArgNullReturn() throws Exception {
    this.setDataset();
    assertThat(dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> r, (o, n) -> null), is(Optional.empty()));
  }

  @Test
  public void testApplyMutation4ArgKeyedNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.applyMutation(IMMEDIATE, null, r -> true, r -> r, (r1, r2) -> r2);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyMutation4ArgKeyedMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .applyMutation(IMMEDIATE, Integer.MAX_VALUE, r -> true, r -> r, (r1, r2) -> r2);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation4ArgKeyedNullValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullValueCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation4ArgKeyedNullDefinitionCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullDefinitionCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation4ArgKeyedMistypedValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.applyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getMistypedCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation4Arg() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");
    BiFunction<Record<String>, Record<String>, Record<String>> biFunction = (o, n) -> n;
    Optional<Record<String>> result = dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> r, biFunction);
    Record<String> newRecord = dataset.get("existing");
    assertThat(result.get(), is(equalTo(newRecord)));
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    result = dataset.tryApplyMutation(IMMEDIATE, "existing",
        r -> true,
        alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))), biFunction);
    newRecord = dataset.get("existing");
    assertThat(result.get(), is(equalTo(newRecord)));
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @Test
  public void testTryApplyMutation4ArgNullReturn() throws Exception {
    this.setDataset();
    assertThat(dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> r, (o, n) -> null), is(Optional.empty()));
  }

  @Test
  public void testTryApplyMutation4ArgKeyedNullKey() throws Exception {
    this.setDataset();
    try {
      assertNotNull(dataset);
      dataset.tryApplyMutation(IMMEDIATE, null, r -> true, r -> r, (r1, r2) -> r2);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTryApplyMutation4ArgKeyedMistypedKey() throws Exception {
    this.setDataset();
    try {
      ((SovereignDataset<Integer>) (Object) dataset)          // unchecked -- intentionally cross typed
          .tryApplyMutation(IMMEDIATE, Integer.MAX_VALUE, r -> true, r -> r, (r1, r2) -> r2);
      fail();
    } catch (ClassCastException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation4ArgKeyedNullValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullValueCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation4ArgKeyedNullDefinitionCell() throws Exception {
    this.setDataset();
    try {
      dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getNullDefinitionCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation4ArgKeyedMistypedValueCell() throws Exception {
    this.setDataset();
    try {
      dataset.tryApplyMutation(IMMEDIATE, "existing", r -> true, r -> singleton(getMistypedCell()), (r1, r2) -> r2);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testTryApplyMutation4ArgWhileLocked() throws Exception {
    this.setDataset();
    String key = "existing";
    try {
      whileLocked(() -> dataset.tryApplyMutation(IMMEDIATE, key, r -> true, r -> r, (r1, r2) -> r2), key);
      fail("Expecting RecordLockedException");
    } catch (RecordLockedException e) {
      // expected
    }
  }


  @Test
  public void testApplyMutation2Arg() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");

    Consumer<Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> r);
    try (Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(mutation);
    }
    Record<String> newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    mutation = dataset.applyMutation(IMMEDIATE, alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))));
    try (Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(mutation);
    }
    newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @Test
  public void testApplyMutation2ArgNullValueCell() throws Exception {
    this.setDataset();
    final Consumer<Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> singleton(getNullValueCell()));
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(mutation);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation2ArgNullDefinitionCell() throws Exception {
    this.setDataset();
    final Consumer<Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> singleton(getNullDefinitionCell()));
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(mutation);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation2ArgMistypedValueCell() throws Exception {
    this.setDataset();
    final Consumer<Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> singleton(getMistypedCell()));
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(mutation);
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgNonKeyed() throws Exception {
    this.setDataset();
    Record<String> oldRecord = dataset.get("existing");
    BiFunction<Record<String>, Record<String>, Record<String>> biFunction = (o, n) -> n;

    Function<Record<String>, Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> r, biFunction);
    try (Stream<Record<String>> stream = dataset.records()) {
      List<Record<String>> records = stream.map(mutation).collect(toList());
      assertFalse(records.isEmpty());
    }
    Record<String> newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord, containsInAnyOrder(oldRecord.toArray()));

    mutation = dataset.applyMutation(IMMEDIATE,
                                     alterRecord(compute(countCell, c -> c.definition().newCell(c.value() + 1))), biFunction);
    try (Stream<Record<String>> stream = dataset.records()) {
      List<Record<String>> records = stream.map(mutation).collect(toList());
      assertFalse(records.isEmpty());
    }
    newRecord = dataset.get("existing");
    assertThat(newRecord, is(not(equalTo(oldRecord))));
    assertThat(newRecord.getKey(), is(equalTo(oldRecord.getKey())));
    assertThat(newRecord.get(countCell), is(Optional.of(2)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyMutation3ArgNonKeyedNullReturn() throws Exception {
    this.setDataset();
    /*
     * Unlike applyMutation(Durability, K, Function, BiFunction), applyMutation(Durability, Function, BiFunction)
     * does not return Optional -- it simply returns the result calculated by the BiFunction ... in this test's
     * case, null.
     */
    Function<Record<String>, Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> r, (o, n) -> null);
    try (Stream<Record<String>> stream = dataset.records()) {
      List<Record<String>> records = stream.map(mutation).collect(toList());
      assertFalse(records.isEmpty());
      assertThat(records, Matchers.everyItem(Matchers.nullValue((Class<Record<String>>)(Object)Record.class)));   // unchecked
    }
  }

  @Test
  public void testApplyMutation3ArgNonKeyedNullValueCell() throws Exception {
    this.setDataset();
    final Function<Record<String>, Record<String>> mutation =
        dataset.applyMutation(IMMEDIATE, r -> { return singleton(getNullValueCell()); },
                              (r1, r2) -> r2);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.map(mutation).collect(toList());
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgNonKeyedNullDefinitionCell() throws Exception {
    this.setDataset();
    final Function<Record<String>, Record<String>> mutation =
        dataset.applyMutation(IMMEDIATE, r -> { return singleton(getNullDefinitionCell()); },
                              (r1, r2) -> r2);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.map(mutation).collect(toList());
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  @Test
  public void testApplyMutation3ArgNonKeyedMistypedValueCell() throws Exception {
    this.setDataset();
    final Function<Record<String>, Record<String>> mutation =
        dataset.applyMutation(IMMEDIATE, r -> { return singleton(getMistypedCell()); },
                              (r1, r2) -> r2);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.map(mutation).collect(toList());
      fail();
    } catch (MalformedCellException e) {
      // expected
    }
  }

  /**
   * This tests whether or not the {@code Function} obtained from
   * {@link SovereignDataset#applyMutation(SovereignDataset.Durability, Function, BiFunction)} can be
   * used in an improper location.
   */
  @Test
  public void testApplyMutation3ArgNonKeyedMisused() throws Exception {
    this.setDataset();
    Function<Record<String>, Record<String>> mutation = dataset.applyMutation(IMMEDIATE, r -> r, (o, n) -> n);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream
          .map(r -> r)            // break away from RecordStreamImpl
          .map(mutation)
          .collect(toList());
    } catch (IllegalStateException e) {
      // Note this exception is thrown during pipeline execution and not construction
      assertThat(e.getMessage(), containsString("applyMutation"));
    }
  }

  @Test
  public void testApplyMutation2ArgNonKeyedMisused() throws Exception {
    this.setDataset();
    Consumer<Record<String>> consumer = dataset.applyMutation(IMMEDIATE, r -> r);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream
          .map(r -> r)            // break away from RecordStreamImpl
          .forEach(consumer);
    } catch (IllegalStateException e) {
      // Note this exception is thrown during pipeline execution and not construction
      assertThat(e.getMessage(), containsString("applyMutation"));
    }
  }

  @Test
  public void testDelete1ArgConsumerMisused() throws Exception {
    this.setDataset();
    Consumer<Record<String>> consumer = dataset.delete(IMMEDIATE);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream
          .map(r -> r)            // break away from RecordStreamImpl
          .forEach(consumer);
    } catch (IllegalStateException e) {
      // Note this exception is thrown during pipeline execution and not construction
      assertThat(e.getMessage(), containsString("delete"));
    }
  }

  @Test
  public void testDelete1ArgConsumer() throws Exception {
    this.setDataset();
    Consumer<Record<String>> consumer = dataset.delete(IMMEDIATE);
    try (final Stream<Record<String>> stream = dataset.records()) {
      stream.forEach(consumer);
    }
    try (final Stream<Record<String>> stream = dataset.records()) {
      assertThat(stream.count(), is((long)0));
    }
  }

  @Test
  public void testDelete2ArgFunction() throws Exception {
    this.setDataset();
    List<Record<String>> expected;
    try (final Stream<Record<String>> stream = dataset.records()) {
      expected = stream.collect(toList());
    }

    List<Record<String>> actual;
    Function<Record<String>, Record<String>> function = dataset.delete(IMMEDIATE, r -> r);
    try (final Stream<Record<String>> stream = dataset.records()) {
      actual = stream.map(function).collect(toList());
    }

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  private void setDataset() {
    this.dataset =
        new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).offheap(4096 * 1024).limitVersionsTo(2).build();
    this.dataset.add(IMMEDIATE, "existing", nameCell.newCell("foo"), countCell.newCell(1));
  }

  /**
   * Gets an {@code Integer} {@code Cell} having a {@code null} {@code CellDefinition}.
   *
   * @return a new {@code Cell} with a {@code null} {@code CellDefinition} and an {@code Integer} value
   */
  private Cell<Integer> getNullDefinitionCell() {
    return new Cell<Integer>() {
      @Override
      public CellDefinition<Integer> definition() {
        return null;
      }

      @Override
      public Integer value() {
        return Integer.MAX_VALUE;
      }
    };
  }

  /**
   * Gets an {@link #countCell} {@code Cell} having a {@code null} value.
   *
   * @return a new {@link #countCell} {@code Cell} with a {@code null} value
   */
  private Cell<Integer> getNullValueCell() {
    return new Cell<Integer>() {
      @Override
      public CellDefinition<Integer> definition() {
        return countCell;
      }

      @Override
      public Integer value() {
        return null;
      }
    };
  }

  /**
   * Gets an {@link #countCell} {@code Cell} having a mistyped value.
   *
   * @return a new {@link #countCell} {@code Cell} with a {@code String} value
   */
  private Cell<Object> getMistypedCell() {
    return new Cell<Object>() {
      @SuppressWarnings("unchecked")
      @Override
      public CellDefinition<Object> definition() {
        return (CellDefinition)countCell;    // unchecked -- intentionally crossed typed
      }

      @Override
      public Object value() {
        return "bad value";
      }
    };
  }

  @Test
  public void testIndexedItemUpdate() throws Exception {
    final CellDefinition<String> name = CellDefinition.define("name", Type.STRING);
    final CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
    final SovereignDataset<String> dataset =
        new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).offheap(4096 * 1024).limitVersionsTo(2).build();
    try {
      final SovereignIndexing indexing = dataset.getIndexing();
      indexing.createIndex(name, SovereignIndexSettings.btree()).call();
      indexing.createIndex(count, SovereignIndexSettings.btree()).call();

      dataset.add(IMMEDIATE, "only", name.newCell("name"), count.newCell(1));
      assertThat(dataset.records().filter(count.value().is(1)).findAny().isPresent(), is(true));

      final Map<String, Cell<?>> originalCells = collectCells(dataset.get("only"));

      final Function<Record<String>, Iterable<Cell<?>>> incrementCount = record -> UpdateOperation.write(count)
              .<String>resultOf(r -> r.get(count).orElse(0) + 1).apply(record);
      dataset.records().filter(name.value().is("name")).forEach(
          dataset.applyMutation(IMMEDIATE, incrementCount));

      final Map<String, Cell<?>> actualCells = collectCells(dataset.get("only"));

      final Set<String> originalCellNames = originalCells.keySet();
      assertThat(actualCells.keySet(), hasItems(originalCellNames.toArray(new String[originalCellNames.size()])));
      assertThat(actualCells.get(count.name()).value(), is(equalTo(2)));
      assertThat(dataset.records().filter(count.value().is(2)).findAny().isPresent(), is(true));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  /**
   * This test runs multiple threads split between read and write operations to the same dataset.
   * <p>
   * Due to a flaw in record version cleanup, this test "pauses" all threads periodically to
   * permit some cleanup to occur. This does not, however, prevent OutOfMemoryError when the
   * test iterations is increased.
   */
  @Test
  public void testGetWhileMutating() throws Exception {
    final int testIterations = 2000;
    final int readIterationMultiple = 2;

    final Timer completionTimer = new Timer("GetWhileMutating_Timer", true);

    final String targetKey = "wigeon";
    final SovereignBuilder<String, SystemTimeReference> builder = AnimalsDataset.getBuilder(8 * 1024 * 1024, true);
    final SovereignDataset<String> dataset = AnimalsDataset.createDataset(builder);
    try {
      AnimalsDataset.addIndexes(dataset);

      final ThreadGroup threadGroup = new ThreadGroup("GetWhileMutating");
      threadGroup.setDaemon(true);

      final int processorCount = Runtime.getRuntime().availableProcessors();
      final int fetchThreadCount = processorCount / 2;
      final int mutationThreadCount = processorCount - fetchThreadCount;

      final Phaser syncPoint = new Phaser(1);

      final ExecutorService executorService =
          Executors.newFixedThreadPool(processorCount,
              new ThreadFactory() {
                private final AtomicInteger threadId = new AtomicInteger(0);

                @Override
                public Thread newThread(@SuppressWarnings("NullableProblems") final Runnable r) {
                  return new Thread(threadGroup, r,
                      String.format("%s %d", threadGroup.getName(), threadId.getAndIncrement()));
                }
              });
      final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

      final List<Runnable> tasks = new ArrayList<>(fetchThreadCount + mutationThreadCount);

      /* Start several threads fetching "wigeon":
       *   1) ensure 'observations' is not decreasing;
       *   2) ensure none of the 'get' operations exceeds 30 seconds.
       *
       * This part of the test avoids AtomicInteger for 'observations' tracking to
       * avoid a test-based synchronization point.
       */
      final long[] lastObservations = new long[fetchThreadCount];
      for (int i = 0; i < fetchThreadCount; i++) {
        final int slot = i;
        Runnable op = () -> {
          final Long observations = dataset.get(targetKey).get(OBSERVATIONS).orElse(0L);
          assertThat(observations, greaterThanOrEqualTo(lastObservations[slot]));
          lastObservations[slot] = observations;
        };
        tasks.add(getTask(op, testIterations * readIterationMultiple, syncPoint, completionTimer));
      }

      /* Start several threads updating 'observations' for "wigeon":
       *   1) ensure none of the 'applyMutation' operations exceeds 30 seconds.
       */
      final Consumer<Record<String>> mutation =
          dataset.applyMutation(IMMEDIATE, record -> UpdateOperation.write(OBSERVATIONS).<String>resultOf(r -> 1 + r.get(OBSERVATIONS).orElse(0L)).apply(record));
      Runnable op = () -> dataset.records()
          .filter(r -> r.getKey().equals(targetKey))
          .forEach(mutation);
      for (int i = 0; i < mutationThreadCount; i++) {
        tasks.add(getTask(op, testIterations, syncPoint, completionTimer));
      }

      /*
       * Now present each of the tasks to the executor.
       */
      syncPoint.bulkRegister(tasks.size());
      final List<Future<Void>> runningTasks = new ArrayList<>(tasks.size());
      runningTasks.addAll(
          tasks.stream().map(task -> completionService.submit(task, null)).collect(toList()));

      syncPoint.arriveAndDeregister();

      /*
       * Wait for any of the tasks to complete.  If a task completes with an exception,
       * cancel the others.
       */
      try {
        for (int i = 0; i < runningTasks.size(); i++) {
          final Future<Void> taskFuture = completionService.take();
          try {
            taskFuture.get();
          } catch (ExecutionException e) {
            final Throwable firstFault = e.getCause();
            runningTasks.stream().filter(future -> !future.isDone()).forEach(future -> future.cancel(true));
            throw new AssertionError(firstFault);
          }
        }
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
      completionTimer.cancel();
    }
  }

  @Test
  public void testStatDump() {
    this.setDataset();
    dataset.getIndexing().createIndex(nameCell, SovereignIndexSettings.BTREE);

    String actual = dataset.getStatsDump();
    String lines[] = actual.split(System.lineSeparator());

    assertThat(lines[0],  startsWith("Memory usage:"));
    assertThat(lines[1],  startsWith("\tHeap Storage:"));
    assertThat(lines[2],  startsWith("\t\tOccupied: "));
    assertThat(lines[3],  startsWith("\t\tAllocated: "));
    assertThat(lines[4],  startsWith("\tPersistent Support Storage:"));
    assertThat(lines[5],  startsWith("\t\tOccupied: "));
    assertThat(lines[6],  startsWith("\t\tAllocated: "));
    assertThat(lines[7],  startsWith("\tPrimary Key Storage:"));
    assertThat(lines[8],  startsWith("\t\tOccupied: "));
    assertThat(lines[9],  startsWith("\t\tAllocated: "));
    assertThat(lines[10], startsWith("\tIndex Storage:"));
    assertThat(lines[11], startsWith("\t\tAllocated: "));
    assertThat(lines[12], startsWith("\tTotal Persistent Size: "));
    assertThat(lines[13], startsWith("Index details:"));
    assertThat(lines[14], startsWith("\tname:"));
    assertThat(lines[15], startsWith("\t\tIndexed Record Count: "));
    assertThat(lines[16], startsWith("\t\tOccupied Storage: "));
    assertThat(lines[17], startsWith("\t\tAccess Count: "));
  }

  /**
   * Returns a {@code Runnable} which repeatedly performs a given operation.
   * <p>
   * The returned {@code Runnable} deregisters from {@code syncPoint} before
   * exiting.
   *
   * @param op the operation to perform
   * @param testIterations the number of times to repeat {@code op}
   * @param syncPoint the {@code Phaser} used to synchronize start of the returned
   *                  {@code Runnable} and "pause" all returned instances after
   *                  performing a number of iterations; the full complement of
   *                  {@code Runnable} instances used must be registered with {@code syncPoint}
   *                  <i>before</i> starting {@code Thread}s with the {@code Runnable}s
   * @param completionTimer the {@code Timer} instance used to cancel the {@code Thread}
   *                        running the returned {@code Runnable} if it takes too long
   *
   * @return a new {@code Runnable}
   */
  private Runnable getTask(final Runnable op,
                           final int testIterations,
                           final Phaser syncPoint,
                           final Timer completionTimer) {
    return () -> {
      final Thread currentThread = Thread.currentThread();
      if (NOISY) {
        System.err.format("[%s] Starting%n", currentThread.getName());
      }

      try {
        try {
          syncPoint.awaitAdvanceInterruptibly(syncPoint.arrive());
        } catch (InterruptedException e) {
          System.err.format("[%s] Interrupted awaiting start: %s", currentThread.getName(), e);
          return;
        }

        try {
          for (int j = 0; j < testIterations; j++) {
            final TimerTask limitTask = new TimerTask() {
              @Override
              public void run() {
                currentThread.interrupt();
              }
            };
            completionTimer.schedule(limitTask, TimeUnit.SECONDS.toMillis(30L));
            op.run();
            limitTask.cancel();
            if (j % 50 == 0) {
              if (NOISY) {
                System.err.format("[%s] Quiescing at %d%n", currentThread.getName(), j);
              }
              try {
                syncPoint.awaitAdvanceInterruptibly(syncPoint.arrive());
              } catch (InterruptedException e) {
                System.err.format("[%s] Interrupted while quiesced: %s%n", currentThread.getName(), e);
                return;
              }
              if (NOISY) {
                System.err.format("[%s] Continuing from %d%n", currentThread.getName(), j);
              }
            }
          }
          if (NOISY) {
            System.err.format("[%s] Completed%n", currentThread.getName());
          }

        } catch (Throwable e) {
          System.err.format("[%s] Failed: %s%n", currentThread.getName(), e);
          throw e;
        }

      } finally {
        syncPoint.arriveAndDeregister();
      }
    };
  }

  private void whileLocked(ThrowingProcedure<RecordLockedException> proc, String key)
      throws RecordLockedException {

    Phaser barrier = new Phaser(2);
    Runnable locker = () -> {
      dataset.records().filter(Record.<String>keyFunction().is(key))
          .forEach(dataset.applyMutation(IMMEDIATE, r -> {
            barrier.arriveAndAwaitAdvance();
            try {
              barrier.awaitAdvanceInterruptibly(barrier.arrive(), 1L, TimeUnit.SECONDS);    // Await proc completion
            } catch (InterruptedException | TimeoutException e) {
              // ignored
            }
            return r;
          }));
      barrier.arriveAndDeregister();    // Permit main to continue
    };
    Thread lockerThread = new Thread(locker);
    lockerThread.setDaemon(true);
    lockerThread.start();

    barrier.arriveAndAwaitAdvance();    // Wait for Record lock to be held
    try {
      proc.invoke();
    } finally {
      barrier.arriveAndAwaitAdvance();      // Permit completion of the "mutation"
      try {
        barrier.awaitAdvanceInterruptibly(barrier.arrive(), 1L, TimeUnit.SECONDS);    // Await background completion
      } catch (InterruptedException | TimeoutException e) {
        // ignored
      }
    }
  }

  private static <K extends Comparable<K>>
  Map<String, Cell<?>> collectCells(final Record<K> record) {
    final LinkedHashMap<String, Cell<?>> cellMap = new LinkedHashMap<>();
    for (final Cell<?> cell : record) {
      cellMap.put(cell.definition().name(), cell);
    }
    return cellMap;
  }

  private static <T extends Exception> void assertThrows(Procedure proc, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  private static <E extends Exception, T extends Exception> void assertThrows(ThrowingProcedure<E> proc, Class<E> permitted, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Throwable t) {
      if (!expected.isInstance(t)) {
        if (t instanceof RuntimeException) {
          throw (RuntimeException)t;
        } else if (t instanceof Error) {
          throw (Error)t;
        } else {
          throw new RuntimeException(t);
        }
      }
    }
  }

  @FunctionalInterface
  private interface Procedure {
    void invoke();
  }

  @FunctionalInterface
  private interface ThrowingProcedure<E extends Exception> {
    void invoke() throws E;
  }
}
