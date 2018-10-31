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
package com.terracottatech.store.systemtest;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventsIT extends BaseClusterWithOffheapAndFRSTest {
  private static final String DATASET = "data";

  @Test
  public void eventsReceived() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();
    try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
      datasetManager.newDataset(DATASET, Type.INT, datasetManager.datasetConfiguration().offheap("primary-server-resource").build());
    }

    int expectedEventCount = 4;

    CyclicBarrier barrier = new CyclicBarrier(2);
    Mutator mutator = new Mutator(barrier);
    Listener listener = new Listener(barrier, expectedEventCount);

    Thread mutatorThread = new Thread(mutator);
    Thread listenerThread = new Thread(listener);

    mutatorThread.start();
    listenerThread.start();

    mutatorThread.join();
    listenerThread.join();

    mutator.checkErrors();
    listener.checkErrors();

    List<String> events = listener.getEvents();
    assertEquals(expectedEventCount, events.size());
    assertTrue(events.contains(":1:ADDITION:"));
    assertTrue(events.contains(":1:DELETION:"));
    assertTrue(events.contains(":2:ADDITION:"));
    assertTrue(events.contains(":2:DELETION:"));
    assertTrue(events.indexOf(":1:ADDITION:") < events.indexOf(":1:DELETION:"));
    assertTrue(events.indexOf(":2:ADDITION:") < events.indexOf(":2:DELETION:"));
  }

  private static class Mutator implements Runnable {
    private final CyclicBarrier barrier;
    private volatile String errorMessage;

    public Mutator(CyclicBarrier barrier) {
      this.barrier = barrier;
    }

    @Override
    public void run() {
      URI connectionURI = CLUSTER.getConnectionURI();
      try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
        try (Dataset<Integer> dataset = datasetManager.getDataset(DATASET, Type.INT)) {
          DatasetWriterReader<Integer> access = dataset.writerReader();

          barrier.await();

          access.add(1);
          access.add(2);
          access.delete(1);
          access.delete(2);
        }
      } catch (Exception e) {
        errorMessage = e.getMessage();
      }
    }

    public void checkErrors() {
      if (errorMessage != null) {
        fail(errorMessage);
      }
    }
  }

  private static class Listener implements Runnable, ChangeListener<Integer> {
    private final CyclicBarrier barrier;
    private final CountDownLatch eventLatch;
    private final List<String> events = new ArrayList<>();
    private volatile String errorMessage;

    public Listener(CyclicBarrier barrier, int expectedEventCount) {
      this.barrier = barrier;
      this.eventLatch = new CountDownLatch(expectedEventCount);
    }

    @Override
    public void run() {
      URI connectionURI = CLUSTER.getConnectionURI();
      try (DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build()) {
        try (Dataset<Integer> dataset = datasetManager.getDataset(DATASET, Type.INT)) {
          DatasetReader<Integer> access = dataset.reader();
          access.registerChangeListener(this);

          barrier.await();
          eventLatch.await(10_000, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        errorMessage = e.getMessage();
      }
    }

    public void checkErrors() {
      if (errorMessage != null) {
        fail(errorMessage);
      }
    }

    @Override
    public synchronized void onChange(Integer key, ChangeType changeType) {
      events.add(":" + key + ":" + changeType + ":");
      eventLatch.countDown();
    }

    @Override
    public void missedEvents() {
      errorMessage = "missed events";
    }

    public List<String> getEvents() {
      return events;
    }
  }
}
