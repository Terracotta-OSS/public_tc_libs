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
package com.terracottatech.sovereign.impl.persistence;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.tool.TinyTester;
import com.terracottatech.tool.TinyTester.TinyStepper;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SovereignTinyPerfTest {
  public final static long TEST_DURATION_SECONDS = TimeUnit.SECONDS.convert(5, TimeUnit.MINUTES);
  public final static int TEST_PAYLOAD_SIZE = 500;
  public final static int TEST_THREADS = Runtime.getRuntime().availableProcessors();
  public final static int TEST_CRUD_KEYCOUNT = 1000000;
  public final static float TEST_CRUD_ADD_PERCENTAGE = .25f;
  public final static float TEST_CRUD_READ_PERCENTAGE = .25f;
  public final static float TEST_CRUD_UPDATE_PERCENTAGE = .25f;
  public final static CellDefinition<byte[]> BYTES_DEF = CellDefinition.defineBytes("bytes");
  public final static CellDefinition<Long> CNT_DEF = CellDefinition.defineLong("cnt");
  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private AbstractStorage storage;

  enum CrudOp {
    ADD,
    READ,
    UPDATE,
    DELETE
  }

  public SovereignDataset<Long> createDataset() {
    return new SovereignBuilder<>(Type.LONG, SystemTimeReference.class).concurrency(Runtime.getRuntime()
                                                                                      .availableProcessors())
      .offheap()
      .storage(storage)
      .diskDurability(SovereignDatasetDiskDurability.timed(100, TimeUnit.MILLISECONDS))
      .build();
  }

  @Before
  public void before() throws IOException, ExecutionException, InterruptedException {
    //this.storage = makeOffheap();
    //this.storage = makeFRS();
    this.storage = makeHybrid();
    this.storage.startupMetadata().get();
    this.storage.startupData().get();
  }

  private AbstractStorage makeHybrid() throws IOException {
    return new SovereignHybridStorage(new PersistenceRoot(tempFolder.newFolder(), PersistenceRoot.Mode.CREATE_NEW),
                                      SovereignBufferResource.unlimited());

  }

  private AbstractStorage makeFRS() throws IOException {
    return new SovereignFRSStorage(new PersistenceRoot(tempFolder.newFolder(), PersistenceRoot.Mode.CREATE_NEW),
                                   SovereignBufferResource.unlimited());
  }

  private AbstractStorage makeOffheap() {
    return new StorageTransient(SovereignBufferResource.unlimited(SovereignBufferResource.MemoryType.OFFHEAP));
  }

  @After
  public void after() throws IOException {
    this.storage.shutdown();
  }

  @Test
  @Ignore
  public void testAppendPerf() {
    SovereignDataset<Long> ds = createDataset();
    final ArrayList<byte[]> payloads = makeRandomByteArrays();
    final AtomicLong base = new AtomicLong(0);
    TinyTester tiny = new TinyTester("Append", TEST_THREADS, () -> new TinyStepper() {
      Random rand = new Random();

      @Override
      public void run() {
        long k = base.incrementAndGet();
        ds.add(SovereignDataset.Durability.LAZY,
               k,
               BYTES_DEF.newCell(payloads.get(rand.nextInt(payloads.size()))),
               CNT_DEF.newCell(k));
      }
    });

    tiny.start(TEST_DURATION_SECONDS, TimeUnit.SECONDS);
    tiny.awaitFinished();
  }

  private CrudOp nextCrudOp(Random r) {
    int per = r.nextInt(100) + 1;
    float sofar = TEST_CRUD_ADD_PERCENTAGE;
    if (per < (int) (100 * sofar)) {
      return CrudOp.ADD;
    }
    sofar = sofar + TEST_CRUD_READ_PERCENTAGE;
    if (per < (int) (100 * sofar)) {
      return CrudOp.READ;
    }
    sofar = sofar + TEST_CRUD_UPDATE_PERCENTAGE;
    if (per < (int) (100 * sofar)) {
      return CrudOp.UPDATE;
    }
    return CrudOp.DELETE;
  }

  @Test
  @Ignore
  public void testCRUDPerf() {
    final ArrayList<byte[]> payloads = makeRandomByteArrays();
    SovereignDataset<Long> ds = createDataset();
    // seed with 1/2 of the keyspace
    {
      Random rand = new Random();
      for (long i = 0; i < TEST_CRUD_KEYCOUNT / 2; i++) {
        ds.add(SovereignDataset.Durability.LAZY,
               i,
               BYTES_DEF.newCell(payloads.get(rand.nextInt(payloads.size()))),
               CNT_DEF.newCell(i));
      }
    }
    final AtomicLong guard = new AtomicLong(0);
    TinyTester tiny = new TinyTester("CRUD", TEST_THREADS, () -> new TinyStepper() {
      Random rand = new Random();
      long counter = 0;
      long adds = 0;
      long reads = 0;
      long updates = 0;
      long deletes = 0;

      @Override
      public void run() {
        long k = rand.nextInt(TEST_CRUD_KEYCOUNT);
        switch (nextCrudOp(rand)) {
          case ADD:
            ds.add(SovereignDataset.Durability.LAZY,
                   k,
                   BYTES_DEF.newCell(payloads.get(rand.nextInt(payloads.size()))),
                   CNT_DEF.newCell(++counter));
            adds++;
            break;
          case READ:
            SovereignRecord<Long> ret = ds.get(k);
            if (ret != null) {
              guard.addAndGet(ret.getKey().longValue());
            }
            reads++;
            break;
          case UPDATE:
            ds.applyMutation(SovereignDataset.Durability.LAZY, k, (r) -> true, (r) -> {
              CellSet cs = new CellSet(r);
              Long was = cs.get(CNT_DEF).get();
              cs.set(CNT_DEF.newCell(was + 1));
              return cs;
            });
            updates++;
            break;
          case DELETE:
            ds.delete(SovereignDataset.Durability.LAZY, k);
            deletes++;
            break;
        }
      }

      @Override
      public String addendumString() {
        return "CRUD: " + adds + " / " + reads + " / " + updates + " / " + deletes;
      }
    });
    tiny.start(TEST_DURATION_SECONDS, TimeUnit.SECONDS);
    tiny.awaitFinished();
    if (guard.get() == 1) {
      // could happen. unlikely though.
      System.out.println("Guard: " + guard.get());
    }
  }

  private ArrayList<byte[]> makeRandomByteArrays() {
    Random rand = new Random();
    final ArrayList<byte[]> payloads = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] b = new byte[TEST_PAYLOAD_SIZE];
      rand.nextBytes(b);
      payloads.add(b);
    }
    return payloads;
  }
}
