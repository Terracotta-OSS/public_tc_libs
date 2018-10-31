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
package com.terracottatech.store.systemtest.ha;

import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.indexing.IndexSettings;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.common.test.Employee.BONUS;
import static com.terracottatech.store.common.test.Employee.CELL_NUMBER;
import static com.terracottatech.store.common.test.Employee.CURRENT;
import static com.terracottatech.store.common.test.Employee.NAME;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class StreamsMutationHAIT extends HABase {

  @Parameterized.Parameters(name = "{0}")
  public static ConfigType[] datasets() {
    return new ConfigType[] {ConfigType.OFFHEAP_ONLY, ConfigType.OFFHEAP_DISK};
  }

  public StreamsMutationHAIT(ConfigType configType) {
    super(configType);
  }

  @Test
  public void testFullyPortableMutationsUnderStreamGetReplicated() throws Exception {
    Random random = new Random();

    Set<String> currentEmployees = new HashSet<>();

    random.ints(1000, -200000, 200001).forEach(x -> {
      boolean isCurrent = random.nextBoolean();
      String name = "Name" + x;
      if (isCurrent) {
        currentEmployees.add(name);
      }
      employeeWriterReader.add(x,
              NAME.newCell(name),
              BONUS.newCell((double) (x * 100)),
              CURRENT.newCell(isCurrent),
              CELL_NUMBER.newCell((long) x));
    });

    employeeWriterReader.records()
            .filter(CURRENT.isFalse())
            .mutate(UpdateOperation.write(CURRENT).value(true));

    employeeWriterReader.records()
            .mutate(UpdateOperation.write(BONUS).value(6000.0));

    assertThat(dataset.getIndexing().getLiveIndexes().size(), is(0));

    dataset.getIndexing().createIndex(CELL_NUMBER, IndexSettings.BTREE).get(5, TimeUnit.SECONDS);

    assertThat(dataset.getIndexing().getLiveIndexes().size(), is(1));

    employeeWriterReader.records()
            .filter(CELL_NUMBER.longValueOrFail().isLessThan(100000))
            .mutate(UpdateOperation.write(BONUS).doubleResultOf(BONUS.doubleValueOrFail().add(1000.0)));

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    employeeReader.records().forEach(x -> {
      assertThat(x.get(CURRENT).get(), is(true));
      assertThat(x.get(BONUS).get(), greaterThanOrEqualTo(6000.0));
    });

    employeeReader.records()
            .filter(CELL_NUMBER.longValueOrFail().isLessThan(100000))
            .forEach(x -> assertThat(x.get(BONUS).get(), is(7000.0)));

  }

  @Test
  public void testFullyPortableMutationsUnderStreamGetReplicatedWithConcurrentCRUD() throws Exception {
    Random random = new Random();

    Set<String> currentEmployees = new HashSet<>();
    Set<Integer> total = new HashSet<>();

    random.ints(1000, -200000, 200001).forEach(x -> {
      boolean isCurrent = random.nextBoolean();
      total.add(x);
      String name = "Name" + x;
      if (isCurrent) {
        currentEmployees.add(name);
      }
      employeeWriterReader.add(x,
              NAME.newCell(name),
              BONUS.newCell((double) (x * 100)),
              CURRENT.newCell(isCurrent),
              CELL_NUMBER.newCell((long) x));
    });

    ExecutorService service = Executors.newWorkStealingPool();

    Future<?> future = service.submit(() -> {
      employeeWriterReader.records()
              .filter(CURRENT.isFalse())
              .mutate(UpdateOperation.write(CURRENT).value(true));

      employeeWriterReader.records()
              .mutate(UpdateOperation.write(BONUS).value(6000.0));

      assertThat(dataset.getIndexing().getLiveIndexes().size(), is(0));

      try {
        dataset.getIndexing().createIndex(CELL_NUMBER, IndexSettings.BTREE).get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new AssertionError(e);
      }

      assertThat(dataset.getIndexing().getLiveIndexes().size(), is(1));

      employeeWriterReader.records()
              .filter(CELL_NUMBER.longValueOrFail().isLessThan(100000))
              .mutate(UpdateOperation.write(BONUS).doubleResultOf(BONUS.doubleValueOrFail().add(1000.0)));
    });

    Future<?> future2 = service.submit(() -> {
      total.forEach(s -> {
        employeeWriterReader.update(s, UpdateOperation.write(NAME.newCell(s + "UPDATED")));
      });
    });


    future.get();
    future2.get();

    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    employeeReader.records().forEach(x -> {
      assertThat(x.get(CURRENT).get(), is(true));
      assertThat(x.get(BONUS).get(), greaterThanOrEqualTo(6000.0));
    });

    employeeReader.records()
            .filter(CELL_NUMBER.longValueOrFail().isLessThan(100000))
            .forEach(x -> assertThat(x.get(BONUS).get(), is(7000.0)));

  }
}
