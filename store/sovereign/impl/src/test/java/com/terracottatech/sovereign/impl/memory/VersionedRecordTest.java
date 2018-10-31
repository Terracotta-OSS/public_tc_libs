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

package com.terracottatech.sovereign.impl.memory;

import org.junit.Test;

import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class VersionedRecordTest {

  @Test
  public void testDeepEquals() throws Exception {
    final MockTimeReference.Generator generator = new MockTimeReference.Generator();

    long msn = 8675309L;
    final String key = "ORIGINAL";

    /*
     * Construction of a VersionedRecord as used here expects the first presented
     * SingleRecord to be the most recent version.
     */
    final List<MockTimeReference> recordTimes = new ArrayList<>();
    final List<Long> msns = new ArrayList<>();
    recordTimes.add(generator.get());  msns.add(msn++);
    recordTimes.add(generator.get());  msns.add(msn++);
    recordTimes.add(generator.get());  msns.add(msn++);

    final LinkedList<SingleRecord<String>> originalRecords = new LinkedList<>();
    originalRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(0), msns.get(0),
        Cell.cell("a", 1), Cell.cell("b", "beef"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2})));
    originalRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(1), msns.get(1),
        Cell.cell("a", 2), Cell.cell("b", "bee"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2})));
    originalRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(2), msns.get(2),
        Cell.cell("a", 3), Cell.cell("b", "be"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2})));
    final VersionedRecord<String> originalRecord = new VersionedRecord<>();
    originalRecord.elements().addAll(originalRecords);

    assertThat(originalRecord.elements().size(), is(3));
    assertThat(originalRecord.deepEquals(originalRecord), is(true));

    final LinkedList<SingleRecord<String>> replicatedRecords = new LinkedList<>();
    replicatedRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(0), msns.get(0),
        Cell.cell("b", "beef"), Cell.cell("a", 1), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2})));
    replicatedRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(1), msns.get(1),
        Cell.cell("a", 2), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2}), Cell.cell("b", "bee")));
    replicatedRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(2), msns.get(2),
        Cell.cell("c", 299_792_458L), Cell.cell("a", 3), Cell.cell("b", "be"), Cell.cell("d", new byte[] { 0, 1, 2 })));
    final VersionedRecord<String> replicatedRecord = new VersionedRecord<>();
    replicatedRecord.elements().addAll(replicatedRecords);

    assertThat(originalRecord.deepEquals(replicatedRecord), is(true));
    assertThat(replicatedRecord.deepEquals(originalRecord), is(true));

    final LinkedList<SingleRecord<String>> alteredRecords = new LinkedList<>();
    alteredRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(0), msns.get(0),
        Cell.cell("b", "beef"), Cell.cell("a", 1), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2})));
    alteredRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(1), msns.get(1),
        Cell.cell("a", 2), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] {0, 1, 2}), Cell.cell("b", "bee")));
    alteredRecords.addFirst(new SingleRecord<>(null, key, recordTimes.get(2), msns.get(2),
        Cell.cell("c", 299_792_458L), Cell.cell("a", 3), Cell.cell("b", "be"), Cell.cell("d", new byte[] { 3, 1, 2 })));
    final VersionedRecord<String> alteredRecord = new VersionedRecord<>();
    alteredRecord.elements().addAll(alteredRecords);

    assertThat(originalRecord.deepEquals(alteredRecord), is(false));
    assertThat(alteredRecord.deepEquals(originalRecord), is(false));
  }

  @Test
  public void testPrune() throws Exception {
    final MockTimeReference.Generator generator = new MockTimeReference.Generator();

    final String key = "key";
    long msn = 0;
    final LinkedList<SingleRecord<String>> versions = new LinkedList<>();
    versions.addFirst(new SingleRecord<>(null, key, generator.get(), ++msn,
        Cell.cell("a", 1), Cell.cell("b", "beef"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] { 0, 1, 2 })));
    versions.addFirst(new SingleRecord<>(null, key, generator.get(), ++msn,
        Cell.cell("a", 2), Cell.cell("b", "bee"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] { 0, 1, 2 })));
    versions.addFirst(new SingleRecord<>(null, key, generator.get(), ++msn,
        Cell.cell("a", 3), Cell.cell("b", "be"), Cell.cell("c", 299_792_458L), Cell.cell("d", new byte[] { 0, 1, 2 })));
    final VersionedRecord<String> record = new VersionedRecord<>();
    record.elements().addAll(versions);

    assertThat(record.elements().size(), is(3));

    final MockTimeReference now = generator.get();

    final MockTimeReferenceVersionLimitStrategy strategy = new MockTimeReferenceVersionLimitStrategy(2);
    final BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> filter =
        strategy.getTimeReferenceFilter();

    record.prune(now, filter);

    assertThat(record.elements().size(), is(2));
    assertThat(record.elements(), is(equalTo(versions.subList(0, 2))));

    for (int i = 0; i < 3; i++) {
      generator.get();
    }
    final MockTimeReference future = generator.get();

    record.prune(future, filter);

    assertThat(record.elements().size(), is(1));
    assertThat(record.elements(), is(equalTo(versions.subList(0, 1))));
  }

  private static final class MockTimeReference implements TimeReference<MockTimeReference> {
    private static final Comparator<MockTimeReference> COMPARATOR = Comparator.comparingInt(t -> t.timeReference);

    private final int timeReference;

    private MockTimeReference(final int timeReference) {
      this.timeReference = timeReference;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return COMPARATOR.compare(this, (MockTimeReference) t);
    }

    public static final class Generator implements TimeReferenceGenerator<MockTimeReference> {
      private static final long serialVersionUID = -2616570502993146053L;

      private final AtomicInteger clock = new AtomicInteger(0);

      @Override
      public Class<MockTimeReference> type() {
        return MockTimeReference.class;
      }

      @Override
      public MockTimeReference get() {
        return new MockTimeReference(this.clock.incrementAndGet());
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }
    }

    @Override
    public String toString() {
      return "MockTimeReference{" +
          "timeReference=" + timeReference +
          '}';
    }
  }

  @SuppressWarnings("rawtypes")
  private static final class MockTimeReferenceVersionLimitStrategy implements VersionLimitStrategy {
    private static final long serialVersionUID = -8350108311747895191L;

    private final int retirementAge;

    private MockTimeReferenceVersionLimitStrategy(final int retirementAge) {
      this.retirementAge = retirementAge;
    }

    @Override
    public Class<MockTimeReference> type() {
      return MockTimeReference.class;
    }

    @Override
    public BiFunction<TimeReference<?>, TimeReference<?>, Retention> getTimeReferenceFilter() {
      return (now, t) -> {
        if (((MockTimeReference)now).timeReference - ((MockTimeReference)t).timeReference > this.retirementAge) {
          return Retention.DROP;
        }
        return Retention.FINISH;
      };
    }
  }
}
