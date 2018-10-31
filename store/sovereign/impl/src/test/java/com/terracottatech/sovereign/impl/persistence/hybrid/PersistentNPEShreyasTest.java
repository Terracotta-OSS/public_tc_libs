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

package com.terracottatech.sovereign.impl.persistence.hybrid;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.sovereign.impl.persistence.PersistenceRoot.Mode.CREATE_NEW;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author shdi
 */
public class PersistentNPEShreyasTest {

  @Rule
  public TemporaryFolder tmpFolder = new org.junit.rules.TemporaryFolder();

  private static int workers = Runtime.getRuntime().availableProcessors();
  private SovereignHybridStorage storage = null;
  private UUID uuid = null;
  String person1 = "p1";

  @Before
  public void before() throws ExecutionException, InterruptedException, IOException {
    File f = tmpFolder.newFolder();
    storage = new SovereignHybridStorage(new PersistenceRoot(f, CREATE_NEW), SovereignBufferResource.unlimited());
    storage.startupMetadata().get();
    storage.startupData().get();
    SovereignDataset<String> ds =
        new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).storage(storage).heap(16 * 1024 * 1024l).build();
    uuid = ds.getUUID();
  }

  @After
  public void efter() throws IOException {
    storage.shutdown();
    storage = null;
  }

  @Test
  public void testBug() throws IOException, InterruptedException, ExecutionException {

    @SuppressWarnings("unchecked")
    SovereignDatasetImpl<String> persons =
        (SovereignDatasetImpl<String>) storage.getDataset(uuid);
    persons.add(SovereignDataset.Durability.IMMEDIATE, person1, Employee.FIRST_NAME.newCell("Marcus"));
    ExecutorService executor = Executors.newCachedThreadPool();
    Runnable tcStoreRunnable = new TCStoreRunnable();

    for (int i = 0; i < workers; i++) {
      executor.execute(tcStoreRunnable);
    }
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    assertThat(persons.records().count(), is(1l));
    SovereignPersistentRecord<String> marcus = persons.get(person1);
    assertThat(marcus.get(Employee.AGE).get(), is(99));
  }

  class TCStoreRunnable implements Runnable {
    @Override
    public void run() {
      @SuppressWarnings("unchecked")
      SovereignDatasetImpl<String> persons =
          (SovereignDatasetImpl<String>) storage.getDataset(uuid);
      persons.add(SovereignDataset.Durability.IMMEDIATE, person1, Employee.FIRST_NAME.newCell("Marcus"));
      for (int i = 0; i < 100; i++) {
        final int finalI = i;
        Record<String> marcus = persons.get(person1);
        persons.applyMutation(SovereignDataset.Durability.IMMEDIATE,
                              marcus.getKey(),
                              r -> true,
                              r -> UpdateOperation.write(Employee.AGE).<String>value(finalI).apply(r));
      }
    }
  }

  static class Employee {

    static final CellDefinition<String> FIRST_NAME = CellDefinition.define("firstName", Type.STRING);
    static final CellDefinition<String> LAST_NAME = CellDefinition.define("lastName", Type.STRING);
    static final CellDefinition<Integer> AGE = CellDefinition.define("age", Type.INT);
    static final CellDefinition<Long> SALARY = CellDefinition.define("salary", Type.LONG);
    static final CellDefinition<Double> NICENESS = CellDefinition.define("niceness", Type.DOUBLE);
    static final CellDefinition<Character> CATEGORY = CellDefinition.define("CATEGORY", Type.CHAR);
    static final CellDefinition<byte[]> LEAVES = CellDefinition.define("leaves", Type.BYTES);
    static final CellDefinition<Boolean> MARRIED = CellDefinition.define("married", Type.BOOL);

    public Cell<String> firstName;
    public Cell<String> lastName;
    public Cell<Integer> age;
    public Cell<Character> category;
    public Cell<Long> salary;
    public Cell<Double> niceness;
    public Cell<Boolean> married;
  }
}
