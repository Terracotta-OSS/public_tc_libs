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
package com.terracottatech.store.systemtest.reconnect;

import com.terracottatech.store.StoreReconnectInterruptedException;
import com.terracottatech.store.systemtest.BaseSystemTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.RecordStream;
import com.terracottatech.test.data.Animals;
import com.terracottatech.testing.rules.EnterpriseCluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests reconnect capability using a single server.
 */
public class SingleServerReconnectIT extends BaseSystemTest {

  @ClassRule
  public static EnterpriseCluster CLUSTER = initCluster("disconnect.xmlfrag");

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

    loadAnimals();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  /**
   * This test exercises the reconnect capabilities using a single server.
   * Multiple threads (and {@link Dataset} handles) using a single {@link DatasetManager}
   * (and thus a single connection) are used.
   */
  @Test
  public void testBasicReconnect() throws Exception {

    List<Task> tasks = new ArrayList<>();

    /*
     * Test reconnect over get operations.
     */
    tasks.add((retryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager) -> {
      boolean deregistered = false;
      List<String> readKeys = new ArrayList<>();
      try {
        try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
          DatasetReader<String> reader = animals.reader();
          for (Animals.Animal animal : Animals.ANIMALS) {
            if (animal.getName().equals("echidna")) {
              sync.arriveAndAwaitAdvance();   // Align before server termination
              sync.arriveAndAwaitAdvance();   // Wait for server to be killed
              sync.arriveAndDeregister();     // Can resume while server is down
              deregistered = true;
            }
            while (true) {
              if (terminated.get()) {
                throw new AssertionError("Test terminated");
              }
              try {
                reader.get(animal.getName()).ifPresent(r -> readKeys.add(r.getKey()));
                break;
              } catch (StoreOperationAbandonedException e) {
                if (retryLimit-- == 0) {
                  throw new AssertionError("Retry limit reached", e);
                }
                System.out.format("%n**********************************************" +
                        "%n[%s] Get abandoned -- retrying" +
                        "%n**********************************************%n%n",
                    Thread.currentThread());
              }
            }
          }
        }
      } catch (AssertionError e) {
        throw e;
      } catch (Throwable e) {
        throw new AssertionError(e);
      } finally {
        if (!deregistered) {
          sync.arriveAndDeregister();
        }
      }

      assertThat("Failed to read all records", readKeys, hasSize(Animals.ANIMALS.size()));
    });

    /*
     * Test reconnect over Indexing/Index access
     */
    tasks.add((retryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager) -> {
      boolean deregistered = false;
      try {
        try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {

          Indexing indexing = animals.getIndexing();
          Collection<Index<?>> liveIndexes = indexing.getLiveIndexes();

          assertThat(liveIndexes, hasSize(2));

          Index<?> taxonomicClassIndex = liveIndexes.stream()
              .filter(i -> i.on().equals(TAXONOMIC_CLASS))
              .findFirst().orElseThrow(() -> new AssertionError("Expected Index not found"));

          for (int i = 0; i < 10; i++) {
            if (i == 5) {
              sync.arriveAndAwaitAdvance();   // Align before server termination
              sync.arriveAndAwaitAdvance();   // Wait for server to be killed
              sync.arriveAndDeregister();     // Can resume while server is down
              deregistered = true;
            }
            while (true) {
              if (terminated.get()) {
                throw new AssertionError("Test terminated");
              }
              try {
                assertThat(taxonomicClassIndex.status(), is(Index.Status.LIVE));
                break;
              } catch (StoreOperationAbandonedException e) {
                if (retryLimit-- == 0) {
                  throw new AssertionError("Retry limit reached", e);
                }
                System.out.format("%n**********************************************" +
                        "%n[%s] Index.status abandoned -- retrying" +
                        "%n**********************************************%n%n",
                    Thread.currentThread());
              }
            }
          }

          Collection<Index<?>> allIndexes = indexing.getAllIndexes();
          for (Index<?> index : allIndexes) {
            System.out.format("Index[%s, %s] = %s%n", index.on(), index.definition(), index.status());
          }
        }
      } catch (AssertionError e) {
        throw e;
      } catch (Throwable e) {
        throw new AssertionError(e);
      } finally {
        if (!deregistered) {
          sync.arriveAndDeregister();
        }
      }
    });

    /*
     * Test reconnect over RecordStream access.  RecordStreams are EXPECTED to fail to
     * continue operations over a reconnection.
     */
    tasks.add((retryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager) -> {
      boolean deregistered = false;
      try {
        try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {

          Spliterator<Record<String>> mammalSpliterator = animals.reader().records()
              .inline()
              .filter(TAXONOMIC_CLASS.value().is("mammal"))
              .spliterator();

          Spliterator<Record<String>> fishSpliterator = animals.reader().records()
              .inline()
              .filter(TAXONOMIC_CLASS.value().is("fish"))
              .spliterator();

          RecordStream<String> amphibianStream = animals.reader().records()
              .inline()
              .filter(TAXONOMIC_CLASS.value().is("amphibian"));

          /*
           * Consume some mammals ...
           */
          List<Record<String>> mammalRecords = new ArrayList<>();
          for (int i = 0; i < 5; i++) {
            mammalSpliterator.tryAdvance(mammalRecords::add);
          }

          /*
           * Now permit the foreground thread to kill & restart the server
           */
          sync.arriveAndAwaitAdvance();   // Permit kill
          sync.arriveAndAwaitAdvance();   // After kill / before restart

          /*
           * In-progress mammal spliterator should fail ...
           */
          try {
            mammalSpliterator.tryAdvance(mammalRecords::add);
            fail("Expecting StoreOperationAbandonedException");
          } catch (StoreOperationAbandonedException e) {
            // expected
          }
          try {
            mammalSpliterator.tryAdvance(mammalRecords::add);
            fail("Expecting StoreOperationAbandonedException");
          } catch (StoreOperationAbandonedException e) {
            // expected
          }
          assertThat(mammalRecords, hasSize(5));

          try {
            fishSpliterator.tryAdvance(r -> { });
            fail("Expecting StoreOperationAbandonedException");
          } catch (StoreOperationAbandonedException e) {
            // expected
          }

          sync.arriveAndAwaitAdvance();   // Await server restart
          sync.arriveAndDeregister();
          deregistered = true;

          try {
            @SuppressWarnings("unused")
            Collection<Record<String>> amphibians = amphibianStream.collect(Collectors.toList());
            fail("Expecting StoreOperationAbandonedException");
          } catch (StoreOperationAbandonedException e) {
            //expected
          }
        }
      } catch (AssertionError e) {
        throw e;
      } catch (Throwable e) {
        throw new AssertionError(e);
      } finally {
        if (!deregistered) {
          sync.arriveAndDeregister();
        }
      }
    });

    testTasks(tasks);
  }

  /**
   * This test exercises the <i>interrupted</i> reconnect capabilities using a single server.
   * Multiple threads (and {@link Dataset} handles) using a single {@link DatasetManager}
   * (and thus a single connection) are used.
   */
  @Test
  public void testReconnectAllInterrupted() throws Exception {

    List<Task> tasks = new ArrayList<>();

    /*
     * Test interrupted reconnect/wait over get operations.  This case lets the
     * interruption abort the operation.
     */
    tasks.add((retryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager) -> {
      boolean deregistered = false;
      interruptibleThreads.add(Thread.currentThread());
      StoreReconnectInterruptedException stopper = null;
      List<String> readKeys = new ArrayList<>();
      try {
        try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
          DatasetReader<String> reader = animals.reader();
          for (Animals.Animal animal : Animals.ANIMALS) {
            if (animal.getName().equals("echidna")) {
              sync.arriveAndAwaitAdvance();   // Align before server termination
              sync.arriveAndAwaitAdvance();   // Wait for server to be killed
              sync.arriveAndDeregister();     // Can resume while server is down
              deregistered = true;
            }
            while (true) {
              if (terminated.get()) {
                throw new AssertionError("Test terminated");
              }
              try {
                reader.get(animal.getName()).ifPresent(r -> readKeys.add(r.getKey()));
                break;
              } catch (StoreOperationAbandonedException e) {
                if (retryLimit-- == 0) {
                  throw new AssertionError("Retry limit reached", e);
                }
                System.out.format("%n**********************************************" +
                        "%n[%s] Get failed - %s; retrying" +
                        "%n**********************************************%n%n",
                    Thread.currentThread(), e.getClass().getSimpleName());
              }
            }
          }
        }
      } catch (StoreReconnectInterruptedException e) {
        stopper = e;
        assertTrue(Thread.interrupted());
        System.out.format("%n**********************************************" +
                "%n[%s] Get failed - %s; retrying" +
                "%n**********************************************%n%n",
            Thread.currentThread(), e.getClass().getSimpleName());
      } catch (AssertionError e) {
        throw e;
      } catch (Throwable e) {
        throw new AssertionError(e);
      } finally {
        if (!deregistered) {
          sync.arriveAndDeregister();
        }
      }

      assertThat(stopper, is(notNullValue()));
      assertThat("Read too many records", readKeys, not(hasSize(Animals.ANIMALS.size())));
    });

    /*
     * Test interrupted reconnect/wait over get operations.  This case attempts to retry
     * the operation after the interrupt.
     */
    tasks.add((retryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager) -> {
      boolean deregistered = false;
      interruptibleThreads.add(Thread.currentThread());
      List<Throwable> storeExceptions = new ArrayList<>();
      List<String> readKeys = new ArrayList<>();
      try {
        try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
          DatasetReader<String> reader = animals.reader();
          for (Animals.Animal animal : Animals.ANIMALS) {
            if (animal.getName().equals("echidna")) {
              sync.arriveAndAwaitAdvance();   // Align before server termination
              sync.arriveAndAwaitAdvance();   // Wait for server to be killed
              sync.arriveAndDeregister();     // Can resume while server is down
              deregistered = true;
            }
            while (true) {
              if (terminated.get()) {
                throw new AssertionError("Test terminated");
              }
              try {
                reader.get(animal.getName()).ifPresent(r -> readKeys.add(r.getKey()));
                break;
              } catch (StoreOperationAbandonedException | StoreReconnectInterruptedException e) {
                if (retryLimit-- == 0) {
                  throw new AssertionError("Retry limit reached", e);
                }
                if (e instanceof StoreReconnectInterruptedException) {
                  assertTrue(Thread.interrupted());
                }
                storeExceptions.add(e);
                System.out.format("%n**********************************************" +
                        "%n[%s] Get failed - %s; retrying" +
                        "%n**********************************************%n%n",
                    Thread.currentThread(), e.getClass().getSimpleName());
              }
            }
          }
        }
      } catch (AssertionError e) {
        throw e;
      } catch (Throwable e) {
        throw new AssertionError(e);
      } finally {
        if (!deregistered) {
          sync.arriveAndDeregister();
        }
      }

      assertThat(storeExceptions, hasItem(instanceOf(StoreReconnectInterruptedException.class)));
      assertThat("Failed to read all records", readKeys, hasSize(Animals.ANIMALS.size()));
    });

    testTasks(tasks);
  }

  /**
   * Run each task in a background thread until all tasks are complete.  For tasks
   * registering themselves as <i>interruptible</i>, interrupt thee task thread
   * before restarting the server.
   * @param tasks the tasks to run
   */
  private void testTasks(List<Task> tasks) throws Exception {
    int globalRetryLimit = 10;
    AtomicBoolean terminated = new AtomicBoolean(false);
    Phaser sync = new Phaser(1);    // One for the main thread; others added later
    Queue<Throwable> backgroundFailures = new ConcurrentLinkedQueue<>();
    List<Thread> interruptibleThreads = new CopyOnWriteArrayList<>();

    ExecutorService executor;

    /*
     * Perform all tests under the same DatasetManager so a shared Connection is used.
     */
    try (DatasetManager manager = DatasetManager.clustered(CLUSTER.getConnectionURI())
        .withConnectionTimeout(2L, TimeUnit.MINUTES).build()) {

      List<Runnable> tests = new ArrayList<>();
      tasks.forEach(t -> tests.add(() -> t.runTask(globalRetryLimit, terminated, sync, interruptibleThreads, backgroundFailures, manager)));

      ThreadGroup group = new ThreadGroup("reconnectTest") {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          backgroundFailures.add(e);
        }
      };

      executor = Executors.newFixedThreadPool(tests.size(), new ThreadFactory() {
        private final AtomicInteger threadCount = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(group, r,
              group.getName() + "_" + String.format("%04d", threadCount.incrementAndGet()));
          thread.setDaemon(true);
          return thread;
        }
      });

      /*
       * Initiate the test tasks created above.
       */
      sync.bulkRegister(tests.size());
      List<Future<?>> taskFutures = new ArrayList<>();
      for (Runnable task : tests) {
        taskFutures.add(executor.submit(task));
      }

      sync.arriveAndAwaitAdvance();   // Synchronize with the test tasks

      // Background thread is in the middle of operations -- kill the server
      CLUSTER.getClusterControl().terminateActive();

      sync.arriveAndAwaitAdvance();  // Permit resumption of operations while server is dead

      TimeUnit.SECONDS.sleep(5L);

      interruptibleThreads.forEach(Thread::interrupt);

      CLUSTER.getClusterControl().startAllServers();
      CLUSTER.getClusterControl().waitForActive();

      sync.arriveAndDeregister();   // Permit resumption of operations awaiting server

      boolean failed = false;
      executor.shutdown();
      try {
        if (executor.awaitTermination(2L, TimeUnit.MINUTES)) {
          /*
           * Collect the results (exceptions) from the test tasks.
           */
          for (Future<?> taskFuture : taskFutures) {
            if (taskFuture.isDone()) {
              try {
                taskFuture.get();
              } catch (ExecutionException e) {
                backgroundFailures.add(e.getCause());
              } catch (Throwable e) {
                backgroundFailures.add(e);
              }
            }
          }
        } else {
          failed = true;
        }

      } catch (InterruptedException e) {
        failed = true;
      } finally {
        if (failed) {
          System.err.format("Background tasks incomplete after 2 minutes; terminating test");
          terminated.set(true);
          executor.shutdownNow();
          backgroundFailures.add(new AssertionError("test timed out"));
        }
      }
    }

    if (!backgroundFailures.isEmpty()) {
      /*
       * Display the background failures -- the AssertionError used below is not always rendered
       * completely and often omits display of the suppressed exceptions.
       */
      System.out.format("%n==================================================================%nFailure stack traces:%n");
      backgroundFailures.forEach(f -> f.printStackTrace(System.out));
      System.out.format("%n==================================================================%n");
      AssertionError fault = new AssertionError(backgroundFailures.size() + " threads failed");
      backgroundFailures.forEach(fault::addSuppressed);
      throw fault;
    }
  }

  private void loadAnimals() throws com.terracottatech.store.StoreException {
    try (DatasetManager manager = DatasetManager.clustered(CLUSTER.getConnectionURI())
        .withConnectionTimeout(1L, TimeUnit.MINUTES).build()) {
      DatasetConfiguration configuration = manager.datasetConfiguration()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .disk(CLUSTER_DISK_RESOURCE)
          .index(TAXONOMIC_CLASS, IndexSettings.btree())
          .index(Animals.Schema.STATUS, IndexSettings.btree())
          .build();
      manager.newDataset("animals", Type.STRING, configuration);

      try (Dataset<String> animals = manager.getDataset("animals", Type.STRING)) {
        DatasetWriterReader<String> writer = animals.writerReader();
        Animals.ANIMALS.forEach(animal -> animal.addTo(writer::add));
      }
    }
  }

  @FunctionalInterface
  private interface Task {
    void runTask(int retryLimit, AtomicBoolean terminated, Phaser sync, List<Thread> interruptibleThreads, Queue<Throwable> backgroundFailures, DatasetManager manager);
  }
}
