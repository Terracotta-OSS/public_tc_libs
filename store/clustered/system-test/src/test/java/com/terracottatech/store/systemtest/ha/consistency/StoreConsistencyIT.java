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
package com.terracottatech.store.systemtest.ha.consistency;

import org.junit.Test;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.test.Employee;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StoreConsistencyIT extends ConsistentHABase {

  public StoreConsistencyIT() {
    super(ConfigType.OFFHEAP_DISK);
  }

  @Test
  public void testReadConsistencyFromRetiredActive() throws Exception {
    Cell<?> cells[] = new Cell<?>[] {
        Employee.NAME.newCell("Pierce Brosnan"),
        Employee.SSN.newCell(007),
        Employee.CURRENT.newCell(false)
    };

    employeeWriterReader.add(1, cells);

    CLUSTER.getClusterControl().terminateOnePassive();
    // With failover-priority = consistency and no other voters available, the active shouldn't have continued as active

    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {

      } finally {
        System.out.println("hippo computed");
      }
    });

    exception.expect(TimeoutException.class);
    future.get(5, TimeUnit.SECONDS);// Should eventually timeout as the dataset get can't complete
  }

}
