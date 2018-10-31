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
package com.terracottatech.sovereign.impl.dataset;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.HEAP;
import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.OFFHEAP;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Performs tests using concurrent streams taken from the same {@link SovereignDataset}.
 */
@RunWith(Parameterized.class)
public class ConcurrentRecordStreamTest {

  private static final boolean VERBOSE = false;

  @Parameterized.Parameters
  public static Collection<TestArgs> args() {
    return Arrays.asList(
        new TestArgs(2, OFFHEAP),
        new TestArgs(8, OFFHEAP),
        new TestArgs(2, HEAP),
        new TestArgs(8, HEAP)
    );
  }

  @Parameterized.Parameter
  public TestArgs args;

  private static final class TestArgs {
    private final int concurrency;
    private final SovereignDataSetConfig.StorageType storageType;

    TestArgs(int concurrency, SovereignDataSetConfig.StorageType storageType) {
      this.concurrency = concurrency;
      this.storageType = storageType;
    }
  }

  @Test
  public void testConcurrentUpdatingStreams() throws Exception {
    int parallelism = Runtime.getRuntime().availableProcessors() * 2;
    long recordCount = 1024L;
    SovereignDataset.Durability durability = SovereignDataset.Durability.IMMEDIATE;
    SovereignDataset<Long> dataset = getDataset(Type.LONG);
    LongCellDefinition longCellDefinition = CellDefinition.defineLong("long");

    final ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

    for (long i = 0L; i < recordCount; i++) {
      dataset.add(durability, i, longCellDefinition.newCell(1L));
    }

    ArrayList<Stream<Record<Long>>> streams = new ArrayList<>();
    for (int i = 0; i < parallelism; i++) {
      Stream<Record<Long>> stream = dataset.records();
      if (VERBOSE) {
        String pattern = "Stream[" + i + "]: %s%n";
        stream = stream.peek(r -> System.out.format(pattern, r));
      }
      streams.add(stream);
    }

    Consumer<Record<Long>> mutation = dataset.applyMutation(durability, r -> {
      CellSet cells = new CellSet(r);
      cells.set(longCellDefinition.newCell(cells.get(longCellDefinition).orElse(0L) + 1L));
      return cells;
    });

    for (Stream<Record<Long>> stream : streams) {
      executorService.execute(() -> stream.forEach(mutation));
    }
    executorService.shutdown();
    executorService.awaitTermination(1L, TimeUnit.MINUTES);

    ArrayList<Long> results = new ArrayList<>();
    for (long i = 0L; i < recordCount; i++) {
      results.add(dataset.get(i).get(longCellDefinition).get());
    }
    assertThat(results, everyItem(is(parallelism + 1L)));
  }

  private <K extends Comparable<K>> SovereignDataset<K> getDataset(Type<K> keyType) {
    SovereignBuilder<K, SystemTimeReference> builder =
        new SovereignBuilder<>(keyType, SystemTimeReference.class).alias("dataset");
    builder = builder.concurrency(args.concurrency);
    switch (args.storageType) {
      case HEAP:
        builder = builder.heap();
        break;
      case OFFHEAP:
        builder = builder.offheap();
        break;
      default:
        throw new AssertionError(args.storageType);
    }

    return builder.build();
  }
}
