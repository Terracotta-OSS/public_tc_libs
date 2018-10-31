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
package com.terracottatech.ehcache.common.frs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


/**
 * Test transactional store and multi-put transactions.
 *
 * @author tim
 * @author RKAV ported from ehcache 2.0 to ehcache 3.0
 */
public class ControlledTransactionRestartStoreTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private File folder;
  private Properties properties;

  @Before
  public void setUp() throws Exception {
    folder = temporaryFolder.newFolder();
    properties = new Properties();
  }

  @Test
  public void testMultiPutTransaction() throws Exception {
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(
              createRestartStore(objectManager));
      RestartableQueue<String> queue1 = createQueue(0, objectManager, restartStore);
      RestartableQueue<String> queue2 = createQueue(1, objectManager, restartStore);

      restartStore.startup().get();

      restartStore.beginControlledTransaction(true, null);

      queue1.add("a");
      queue2.add("b");

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(
              createRestartStore(objectManager));
      RestartableQueue<String> queue1 = createQueue(0, objectManager, restartStore);
      RestartableQueue<String> queue2 = createQueue(1, objectManager, restartStore);

      restartStore.startup().get();

      assertNull(queue1.poll());
      assertNull(queue2.poll());

      restartStore.beginControlledTransaction(true, null);

      queue1.add("foo");
      queue2.add("foo");

      restartStore.commitControlledTransaction(null);

      restartStore.beginControlledTransaction(true, null);

      queue1.add("bar");
      queue2.add("bar");

      restartStore.commitControlledTransaction(null);

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(
              createRestartStore(objectManager));
      RestartableQueue<String> queue1 = createQueue(0, objectManager, restartStore);
      RestartableQueue<String> queue2 = createQueue(1, objectManager, restartStore);

      restartStore.startup().get();

      assertThat(queue1.poll(), is("foo"));
      assertThat(queue2.poll(), is("foo"));
      assertThat(queue1.poll(), is("bar"));
      assertThat(queue2.poll(), is("bar"));

      restartStore.shutdown();
    }
  }

  @Test
  public void testGlobalTransactionSpanningThreads() throws Exception {
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      final ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(
              createRestartStore(objectManager));
      final RestartableQueue<String> queue1 = createQueue(0, objectManager, restartStore);
      final RestartableQueue<String> queue2 = createQueue(1, objectManager, restartStore);

      restartStore.startup().get();

      assertNull(queue1.poll());
      assertNull(queue2.poll());
      final ByteBuffer id = ByteBuffer.wrap("test".getBytes());

      Thread thread = new Thread(() -> restartStore.beginGlobalTransaction(true, id));
      thread.start();
      thread.join();

      ExecutorService executor = Executors.newFixedThreadPool(10);
      List<Future<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        tasks.add(executor.submit(() -> {
          queue1.add("foo");
          queue2.add("bar");
        }));
      }

      for (Future<?> f : tasks) {
        f.get();
      }

      thread = new Thread(() -> {
        try {
          restartStore.commitGlobalTransaction(id);
        } catch (TransactionException e) {
          throw new RuntimeException(e);
        }
      });
      thread.start();
      thread.join();

      restartStore.shutdown();
    }
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(
              createRestartStore(objectManager));
      RestartableQueue<String> queue1 = createQueue(0, objectManager, restartStore);
      RestartableQueue<String> queue2 = createQueue(1, objectManager, restartStore);

      restartStore.startup().get();

      assertThat(queue1.poll(), is("foo"));
      assertThat(queue1.poll(), is("foo"));
      assertThat(queue2.poll(), is("bar"));
      assertThat(queue2.poll(), is("bar"));

      restartStore.shutdown();
    }
  }

  @Test
  public void testNestedControlledTransactionsThrows() throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
    ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        new ControlledTransactionRestartStore<>(createRestartStore(objectManager));
    restartStore.startup().get();

    restartStore.beginControlledTransaction(true, null);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Nested controlled transactions are not supported.");
    try {
      restartStore.beginControlledTransaction(true, null);
    } finally {
      restartStore.shutdown();
    }
  }

  @Test
  public void testControlledTransactionExtraCommitThrows() throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
    ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        new ControlledTransactionRestartStore<>(createRestartStore(objectManager));
    restartStore.startup().get();

    restartStore.beginControlledTransaction(true, null);
    restartStore.commitControlledTransaction(null);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Transaction is not started");
    try {
      restartStore.commitControlledTransaction(null);
    } finally {
      restartStore.shutdown();
    }
  }

  @Test
  public void testNestedControlledTransactionsInGlobalTransaction() throws Exception {
    final ByteBuffer id1 = ByteBuffer.wrap("test1".getBytes());
    final ByteBuffer id2 = ByteBuffer.wrap("test2".getBytes());
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(createRestartStore(objectManager));
      RestartableQueue<String> queue = createQueue(0, objectManager, restartStore);
      restartStore.startup().get();

      restartStore.beginGlobalTransaction(true, id1);

      restartStore.beginControlledTransaction(true, id1);
      queue.offer("foo");
      restartStore.commitControlledTransaction(id1);

      restartStore.beginControlledTransaction(true, id1);
      queue.offer("bar");
      restartStore.commitControlledTransaction(id1);

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(createRestartStore(objectManager));
      RestartableQueue<String> queue = createQueue(0, objectManager, restartStore);
      restartStore.startup().get();

      assertThat(queue.poll(), nullValue());

      restartStore.beginGlobalTransaction(true, id1);

      restartStore.beginControlledTransaction(true, id2);
      queue.offer("foo");
      queue.offer("bar");
      // this should commit everything even if global txn is not closed
      restartStore.commitControlledTransaction(id2);

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
      ControlledTransactionRestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
          new ControlledTransactionRestartStore<>(createRestartStore(objectManager));
      RestartableQueue<String> queue = createQueue(0, objectManager, restartStore);
      restartStore.startup().get();

      assertThat(queue.poll(), is("foo"));
      assertThat(queue.poll(), is("bar"));

      restartStore.shutdown();
    }

  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createRestartStore(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws
      RestartStoreException, IOException {
    return RestartStoreFactory.createStore(objectManager, folder, properties);
  }

  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<>();
  }

  private RestartableQueue<String> createQueue(int id,
                                               RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                               RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability) {
    RestartableQueue<String> restartableQueue = new RestartableQueue<>(newId(id), restartability, String.class);
    objectManager.registerObject(restartableQueue);
    return restartableQueue;
  }

  private ByteBuffer newId(int id) {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(id).flip();
    return buf;
  }
}
