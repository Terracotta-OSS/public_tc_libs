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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.exceptions.LockConflictException;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.tool.Diagnostics;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.intrinsics.impl.CellExtractor.extractComparable;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author mscott
 */
public abstract class AbstractMemoryIntegrationTest {


  public AbstractMemoryIntegrationTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  abstract SovereignDataset<Integer> createDataset();

  @Test
  public void testConcurrentMutation() throws IOException {
    SovereignDataset<Integer> data = createDataset();
    try {
      CellDefinition<String> tag = define("tag", Type.STRING);
      CellDefinition<Integer> count = define("count", Type.INT);
      CellDefinition<Integer> series = define("series", Type.INT);
      BoolCellDefinition mutator = defineBool("mutator");
      data.add(IMMEDIATE, 1, tag.newCell("CD"), count.newCell(1), series.newCell(1));
      data.add(IMMEDIATE, 2, tag.newCell("CS"), count.newCell(1), series.newCell(2));
      data.add(IMMEDIATE, 3, tag.newCell("AS"), count.newCell(1), series.newCell(3));
      data.add(IMMEDIATE, 4, tag.newCell("CJ"), count.newCell(1), series.newCell(4));
      data.add(IMMEDIATE, 5, tag.newCell("MS"), count.newCell(1), series.newCell(5));

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(extractComparable(series).isLessThanOrEqualTo(3)).count(), is(3L));
      }

      final Semaphore s = new Semaphore(0);
      new Thread(() -> {
        try {
          s.acquire(1);
          data.applyMutation(IMMEDIATE, 3, r -> true, r -> replaceOrAdd(r,
                                                                        count.newCell(r.get(count).get() + 1),
                                                                        mutator.newCell(false)));
          s.release(2);
        } catch (Exception ie) {
          ie.printStackTrace();
        }
      }, "singular").start();

// will iterate the entire set of 5; the number of updates done here should be 4 as the thread above does one.
      try (final Stream<Record<Integer>> recordStream = data.records()) {
        recordStream.filter(extractComparable(count).isLessThan(2))
            .forEach(data.applyMutation(IMMEDIATE, r -> {
              /*
               * When processing record 1, allow other thread to process record 3 and await its completion.
               */
              if (r.getKey() == 1) {
                s.release();
                try {
                  while (!s.tryAcquire(2, 10L, TimeUnit.NANOSECONDS)) {
                    Thread.yield();
                  }
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
              return replaceOrAdd(r,
                  count.newCell(r.get(count).get() + 1),
                  mutator.newCell(true));
            }));
      }

//      try (final Stream<Record<Integer>> recordStream = data.records()) {
//        recordStream.forEach(r -> System.out.println(r.get(count)));
//      }

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(mutator.isTrue()).count(), is(4L));
      }

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(mutator.isFalse()).count(), is(1L));
      }

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.mapToInt(r -> r.get(count).get()).reduce(Integer::sum).getAsInt(), is(10));
      }

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }
  @Test
  public void testConcurrentDeletion() throws InterruptedException, IOException {
    for (int i = 0; i < 500; i++) {
      innerConcurrentDeletion();
    }
  }

  private void innerConcurrentDeletion() throws InterruptedException, IOException {
    SovereignDataset<Integer> data = createDataset();
    try {
      CellDefinition<String> tag = define("tag", Type.STRING);
      CellDefinition<Integer> count = define("count", Type.INT);
      CellDefinition<Integer> series = define("series", Type.INT);
      BoolCellDefinition mutator = defineBool("mutator");
      data.add(IMMEDIATE, 1, tag.newCell("CD"), count.newCell(1), series.newCell(1));
      data.add(IMMEDIATE, 2, tag.newCell("CS"), count.newCell(1), series.newCell(2));
      data.add(IMMEDIATE, 3, tag.newCell("AS"), count.newCell(1), series.newCell(3));
      data.add(IMMEDIATE, 4, tag.newCell("CJ"), count.newCell(1), series.newCell(4));
      data.add(IMMEDIATE, 5, tag.newCell("MS"), count.newCell(1), series.newCell(5));

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(extractComparable(series).isLessThanOrEqualTo(3)).count(), is(3L));
      }

      final Semaphore s = new Semaphore(0);
      Thread thr = new Thread(() -> {
        try {
          s.acquire(2);
          data.delete(IMMEDIATE, 3);
        } catch (Exception ie) {
          ie.printStackTrace();
        }
      }, "singular");
      thr.start();

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        recordStream.filter(extractComparable(count).isLessThan(2))
            .forEach(data.applyMutation(IMMEDIATE, r -> {
              s.release();
              return replaceOrAdd(r,
                  count.newCell(r.get(count).get() + 1),
                  mutator.newCell(true));
            }));
      }
      thr.join();

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(mutator.isTrue()).count(), is(4L));
      }

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.mapToInt(r -> r.get(count).get()).reduce(Integer::sum).getAsInt(), is(8));
      }

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }

  @Test
  public void testAddThenDelete() throws Exception {
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final CellDefinition<String> tag = define("tag", Type.STRING);
      final CellDefinition<Integer> count = define("count", Type.INT);
      final CellDefinition<Integer> series = define("series", Type.INT);
      dataset.add(IMMEDIATE, 1, tag.newCell("CJ"), count.newCell(0), series.newCell(1));
      dataset.delete(IMMEDIATE, 1);
      assertNull(dataset.get(1));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testFastMutation() throws Exception {
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final CellDefinition<String> tag = CellDefinition.define("tag", Type.STRING);
      final CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
      final CellDefinition<Integer> series = CellDefinition.define("series", Type.INT);

      dataset.add(IMMEDIATE, 1, tag.newCell("CJ"), count.newCell(0), series.newCell(1));
      Thread t = new Thread(()-> {
          for (int i = 1; i <= 1000; i++) {
            final int c = i;
            dataset.applyMutation(IMMEDIATE, 1, r -> true, r->replaceOrAdd(r, count.newCell(c)));
          }
        }
      );
      t.start();
      while (t.isAlive()) {
        try (final Stream<Record<Integer>> recordStream = dataset.records()) {
          assertThat(recordStream.count(), is(1L));
        }
      }
      t.join();
      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        assertEquals(1L, recordStream.count());
      }

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testVersionGC() throws Exception {
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final CellDefinition<String> tag = CellDefinition.define("tag", Type.STRING);
      final CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
      final CellDefinition<Integer> series = CellDefinition.define("series", Type.INT);

      dataset.add(IMMEDIATE, 1, tag.newCell("CJ"), count.newCell(0), series.newCell(1));
      for (int i = 1; i <= 50; i++) {
        final int c = i;
        dataset.applyMutation(IMMEDIATE, 1, r -> true, r->replaceOrAdd(r, count.newCell(c)));
      }

      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        assertEquals("Full dataset:", 1L, recordStream.count());
      }

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testVersioning() throws IOException, InterruptedException {
    final SovereignDataset<Integer> data = createDataset();
    try {
      CellDefinition<String> tag = CellDefinition.define("tag", Type.STRING);
      CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
      CellDefinition<Integer> series = CellDefinition.define("series", Type.INT);
      data.add(IMMEDIATE, 1, tag.newCell("CD"), count.newCell(1), series.newCell(1));
      data.add(IMMEDIATE, 2, tag.newCell("CS"), count.newCell(1), series.newCell(2));
      data.add(IMMEDIATE, 3, tag.newCell("AS"), count.newCell(1), series.newCell(3));
      data.add(IMMEDIATE, 4, tag.newCell("CJ"), count.newCell(1), series.newCell(4));
      data.add(IMMEDIATE, 5, tag.newCell("MS"), count.newCell(1), series.newCell(5));

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.filter(extractComparable(series).isLessThanOrEqualTo(3)).count(), is(3L));
      }

      final Semaphore s = new Semaphore(0);
      final Thread thread = new Thread(() -> {
        try {
          s.acquire(2);
          data.applyMutation(IMMEDIATE, 3, r -> true, r -> replaceOrAdd(r, count.newCell(r.get(count).get() + 1)));
        } catch (Exception ie) {
          ie.printStackTrace();
        }
      }, "singular");
      thread.start();

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        recordStream.filter(extractComparable(count).isLessThan(2)).forEach(r-> {
          s.release();
//          System.out.println(r.get(tag));
        });
      }
      thread.join();

      try (final Stream<Record<Integer>> recordStream = data.records()) {
        assertThat(recordStream.mapToInt(r -> r.get(count).get()).reduce(Integer::sum).getAsInt(), is(6));
      }

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }

  /**
   * A contrived scenario to test deadlocked Record lock acquisition for Stream operations.
   */
  @Test
  public void testDeadlock() throws Exception {
    final CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final long deadlockWait =
          TimeUnit.SECONDS.toMillis(5L) + ((SovereignDatasetImpl)dataset).getConfig().getRecordLockTimeout();
      dataset.add(IMMEDIATE, 1);

      final ExecutorService executorService = Executors.newFixedThreadPool(3, new ThreadFactory() {
        private int count = 0;
        @Override
        public Thread newThread(final Runnable r) {
          return new Thread(r, "testDeadlock[" + ++count + "]");
        }
      });

      final Semaphore s = new Semaphore(0);

      final Collection<Callable<Void>> tasks = new ArrayList<>();
      tasks.add(() -> {
        dataset.applyMutation(IMMEDIATE, 1, r -> true, r -> {
          s.release();    // Permit other thread to run
          try {
            // Induce a deadlock since we're holding the lock for record 1
            while (!s.tryAcquire(2, 500L, TimeUnit.NANOSECONDS)) {
              Thread.yield();
            }
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
          return replaceOrAdd(r, count.newCell(r.get(count).orElse(0) + 1));
        });
        return null;
      });

      final AtomicBoolean caughtLockConflictException = new AtomicBoolean(false);
      tasks.add(() -> {
        try {
          s.acquire();
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
        try {
          // Should deadlock waiting for record lock
          dataset.applyMutation(IMMEDIATE, 1, r -> true, r -> replaceOrAdd(r, count.newCell(r.get(count).orElse(0) + 1)));
        } catch (LockConflictException e) {
          caughtLockConflictException.set(true);
        } finally {
          s.release(2);     // Release the other thread from deadlock
        }
        return null;
      });

      /*
       * 1/2 second before the potentially deadlocked tasks are cancelled, a thread dump is taken.
       */
      final Future<?> dumpFuture = executorService.submit(() -> {
        try {
          TimeUnit.MILLISECONDS.sleep(deadlockWait - 500);
          Diagnostics.threadDump();
        } catch (InterruptedException e) {
          // Ignored
        }
      });

      final List<Future<Void>> futures = executorService.invokeAll(tasks, deadlockWait, TimeUnit.MILLISECONDS);
      dumpFuture.cancel(false);

      AssertionError testFailure  = null;
      for (final Future<Void> future : futures) {
        try {
          future.get();
        } catch (CancellationException | InterruptedException | ExecutionException e) {
          if (testFailure == null) {
            testFailure = new AssertionError(e);
          } else {
            testFailure.addSuppressed(e);
          }
        }
      }
      if (testFailure != null) {
        throw testFailure;
      }

      assertThat(caughtLockConflictException.get(), is(true));
      assertThat(dataset.get(1).get(count).get(), is(1));

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  /**
   * Ensure a lock operation interrupted by {@link Thread#interrupt()} is <b>not</b> interrupted
   * but continues to wait for the full {@link SovereignDatasetImpl#recordLockTimeout} value and
   * that the thread retained the interrupted status.
   */
  @Test
  public void testInterruptedLock() throws Exception {
    final CellDefinition<Integer> count = CellDefinition.define("count", Type.INT);
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final long recordLockTimeout = ((SovereignDatasetImpl)dataset).getConfig().getRecordLockTimeout();

      dataset.add(IMMEDIATE, 1);

      final Map<Thread, Throwable> uncaughtExceptions = new HashMap<>();
      final ThreadGroup group = new ThreadGroup("interruptTestThreads") {
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
          uncaughtExceptions.put(t, e);
        }
      };

      final Semaphore s = new Semaphore(0);

      /*
       * This thread attempts to mutate an existing record (1).  Inside the mutation it:
       * (1) adds a semaphore unit to permit 'blockedThread' to start
       * (2) acquire two semaphore units (to be added when 'blockedThread' runs)
       * (3) add 1 to the 'count' field in record (1)
       */
      final Thread blockingThread = new Thread(group, () -> {
        dataset.applyMutation(IMMEDIATE, 1, r -> true, r -> {
          s.release();    // Permit other thread to run
          try {
            // Induce a deadlock since we're holding the lock for record 1
            while (!s.tryAcquire(2, 500L, TimeUnit.NANOSECONDS)) {
              Thread.yield();
            }
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
          System.out.println("Blocking thread lock acquired...");
          return replaceOrAdd(r, count.newCell(r.get(count).orElse(0) + 1));
        });
      }, "blockingThread");
      blockingThread.setDaemon(true);
      blockingThread.start();

      /*
       * This thread attempts to mutate an existing record (1).  Before attempting
       * the mutation, it obtains a single semaphore unit (added by 'blockingThread').
       * Following the mutation, it adds two semaphore units to permit 'blockingThread'
       * to continue.
       */
      final AtomicBoolean isInterrupted = new AtomicBoolean(false);
      final AtomicLong waitDuration = new AtomicLong(-1);
      final Thread blockedThread = new Thread(group, () -> {
        try {
          s.acquire();
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
        final long startTime = System.nanoTime();
        try {
          // Should deadlock waiting for record lock
          System.out.println("Blocked thread starting...");
          dataset.applyMutation(IMMEDIATE, 1, r -> true, r -> replaceOrAdd(r, count.newCell(r.get(count).orElse(0) + 1)));
        } finally {
          System.out.println("Blocked thread done... " + Thread.currentThread().isInterrupted());
          waitDuration.set(System.nanoTime() - startTime);
          isInterrupted.set(Thread.currentThread().isInterrupted());
          s.release(2);     // Release the other thread from deadlock
        }
      }, "blockedThread");
      blockedThread.setDaemon(true);
      blockedThread.start();
      assertThat(blockedThread.isInterrupted(), is(false));

      /*
       * Interrupt 'blockedThread' after some delay
       */
      final long delay = recordLockTimeout / 20;
      final Timer pokeTimer = new Timer("pokeTimer", true);
      pokeTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          if (blockedThread.getState() == Thread.State.TERMINATED) {
            System.out.println("Error: tried to interrupt terminated thread");
            return;
          }
          blockedThread.interrupt();
          System.out.println("Interrupted.");
        }
      }, delay);

      /*
       * Await completion of 'blockedThread'.  This thread should
       * (1) be completed *before* the join time expires,
       * (2) have been interrupted
       * (3) have waited at least 'recordLockTimeout' milliseconds
       * (4) have terminated with a LockConflictException
       */
      try {
        System.out.println("Waiting for blocked thread to proceed");
        blockedThread.join(TimeUnit.SECONDS.toMillis(1L) + recordLockTimeout);
      } catch (InterruptedException e) {
        fail("blockedThread failed to terminate with LockConflictException within " + recordLockTimeout + " ms");
      } finally {
        pokeTimer.cancel();
      }

      assertThat(isInterrupted.get(), is(true));
      assertThat(waitDuration.get(), is(Matchers.greaterThanOrEqualTo(recordLockTimeout)));
      assertThat(uncaughtExceptions.get(blockedThread), is(instanceOf(LockConflictException.class)));

      /*
       * Wait for the 'blockingThread' to complete.  This thread should complete "normally"
       * after termination of 'blockedThread'.
       */
      try {
        System.out.println("Waiting for blocking thread to proceed");
        blockingThread.join(TimeUnit.SECONDS.toMillis(1L));
      } catch (InterruptedException e) {
        // Ignored
      }

      final Optional<Integer> countValue = dataset.get(1).get(count);
      assertThat(countValue.isPresent(), is(true));
      assertThat(countValue.get(), is(1));

      assertThat(uncaughtExceptions.get(blockingThread), is(nullValue()));

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  static Iterable<Cell<?>> remove(Record<?> record, CellDefinition<?>... values) {
    HashMap<String, Cell<?>> list = new HashMap<>();
    for (Cell<?> item : record) {
      list.put(item.definition().name(), item);
    }
    for (CellDefinition<?> item : values) {
      list.remove(item.name());
    }
    return list.values();
  }

  static Iterable<Cell<?>> replaceOrAdd(Record<?> record, Cell<?>... values) {
    HashMap<String, Cell<?>> list = new HashMap<>();
    for (Cell<?> item : record) {
      list.put(item.definition().name(), item);
    }
    for (Cell<?> item : values) {
      list.put(item.definition().name(), item);
    }
    return list.values();
  }
}
