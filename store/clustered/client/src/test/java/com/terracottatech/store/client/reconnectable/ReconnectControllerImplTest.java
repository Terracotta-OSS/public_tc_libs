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
package com.terracottatech.store.client.reconnectable;

import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.StoreReconnectFailedException;
import com.terracottatech.store.StoreReconnectInterruptedException;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.exception.ConnectionClosedException;
import org.terracotta.exception.ConnectionShutdownException;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link ReconnectControllerImpl}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReconnectControllerImplTest {

  @Mock
  private ReconnectableLeasedConnection connection;

  @Test
  public void testNonThrowingOperationHappyPath() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    AtomicBoolean invoked = new AtomicBoolean(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });

    assertTrue(invoked.get());
    verify(connection, never()).reconnect();
    assertNull(replacementController.get());

    invoked.set(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });
    verify(connection, never()).reconnect();
  }

  @Test
  public void testNonThrowingOperationException() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    try {
      reconnectController.withConnectionClosedHandling(() -> {
        throw new RuntimeException("raised");
      });
      fail("Expecting RuntimeException");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), is("raised"));
    }

    verify(connection, never()).reconnect();
    assertNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });

    assertTrue(invoked.get());
    verify(connection, never()).reconnect();
  }

  @Test
  public void testNonThrowingOperationConnectionClosedException() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    induceReconnect(reconnectController);

    verify(connection, times(1)).reconnect();
    assertNotNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });

    assertTrue(invoked.get());
    verify(connection, times(1)).reconnect();
  }

  @Test
  public void testNonThrowingOperationConnectionClosedExceptionFailedReconnect() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    RuntimeException reconnectFailure = new RuntimeException("reconnect failure");
    doThrow(reconnectFailure).when(connection).reconnect();
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    try {
      reconnectController.withConnectionClosedHandling(() -> {
        throw new ConnectionClosedException("raised");
      });
      fail("Expecting StoreReconnectFailedException");
    } catch (StoreReconnectFailedException e) {
      assertThat(e.getCause(), is(reconnectFailure));
      assertThat(e.getSuppressed(), is(matchConnectionClosedException()));
    }

    verify(connection, times(1)).reconnect();
    assertNotNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    try {
      reconnectController.withConnectionClosedHandling(() -> {
        invoked.set(true);
        return null;
      });
    } catch (StoreReconnectFailedException e) {
      assertThat(e.getCause(), is(reconnectFailure));
      assertThat(e.getSuppressed(), is(matchConnectionClosedException()));
    }

    assertFalse(invoked.get());
    verify(connection, times(1)).reconnect();
  }

  @SuppressWarnings("unchecked")
  public Matcher<Throwable[]> matchConnectionClosedException() {
    return arrayContaining(instanceOf(ConnectionClosedException.class));
  }

  @Test
  public void testNonThrowingOperationConnectionClosedExceptionInterruptedReconnect() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    InterruptedException reconnectFailure = new InterruptedException("simulated reconnection interruption");
    doThrow(reconnectFailure).when(connection).reconnect();
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    try {
      reconnectController.withConnectionClosedHandling(() -> {
        throw new ConnectionClosedException("raised");
      });
      fail("Expecting StoreReconnectInterrupted Exception");
    } catch (StoreReconnectInterruptedException e) {
      assertThat(e.getCause(), is(reconnectFailure));
      assertThat(e.getSuppressed(), is(matchConnectionClosedException()));
      assertTrue(Thread.interrupted());
    }

    verify(connection, times(1)).reconnect();
    assertNull(replacementController.get());

    /*
     * Retry the failing operation without interrupting the reconnect; this should permit
     * completion of the reconnection with replacement of the controller.
     */
    doNothing().when(connection).reconnect();
    induceReconnect(reconnectController);

    verify(connection, times(2)).reconnect();
    assertNotNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });

    assertTrue(invoked.get());
    verify(connection, times(2)).reconnect();
  }

  @Test
  public void testNonThrowingOperationConnectionShutdownException() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    try {
      reconnectController.withConnectionClosedHandling(() -> {
        throw new ConnectionShutdownException("raised");
      });
    } catch (StoreOperationAbandonedException e) {
      assertThat(e.getCause(), is(instanceOf(ConnectionShutdownException.class)));
    }

    verify(connection, times(1)).reconnect();
    assertNotNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    reconnectController.withConnectionClosedHandling(() -> {
      invoked.set(true);
      return null;
    });

    assertTrue(invoked.get());
    verify(connection, times(1)).reconnect();
  }

  @Test
  public void testNonThrowingOperationClosedConnection() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    reconnectController.close();
    assertNotNull(replacementController.get());

    AtomicBoolean invoked = new AtomicBoolean(false);
    try {
      reconnectController.withConnectionClosedHandling(() -> {
        invoked.set(true);
        return null;
      });
    } catch (ConnectionClosedException e) {
      // expected
    }

    assertFalse(invoked.get());
    verify(connection, never()).reconnect();
  }

  @Test
  public void testNonThrowingOperationAwakensAfterReconnect() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    CyclicBarrier syncPoint = new CyclicBarrier(2);
    AtomicBoolean backgroundInvoked = new AtomicBoolean();
    Thread otherOperation = new Thread(() -> {
      try {
        syncPoint.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new AssertionError(e);
      }
      reconnectController.withConnectionClosedHandling(() -> {
        backgroundInvoked.set(true);
        return null;
      });
    });
    otherOperation.setDaemon(true);

    doAnswer(invocationOnMock -> {
      // This is slightly evil but kick off a background operation to ensure we're blocked
      otherOperation.start();
      syncPoint.await();
      return null;
    }).when(connection).reconnect();

    induceReconnect(reconnectController);
    assertNotNull(replacementController.get());

    assertThatEventually(() -> {
      try {
        otherOperation.join();
        return backgroundInvoked.get();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }, is(true)).within(ofSeconds(30));
  }

  @Test
  public void testNonThrowingOperationAwakensAfterInterruptedReconnect() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    CyclicBarrier syncPoint = new CyclicBarrier(2);
    AtomicBoolean backgroundInvoked = new AtomicBoolean();
    Thread otherOperation = new Thread(() -> {
      try {
        syncPoint.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new AssertionError(e);
      }
      reconnectController.withConnectionClosedHandling(() -> {
        backgroundInvoked.set(true);
        return null;
      });
    });
    otherOperation.setDaemon(true);

    doAnswer(invocationOnMock -> {
      // This is slightly evil but kick off a background operation to ensure we're blocked
      otherOperation.start();
      syncPoint.await();
      throw new InterruptedException("simulated reconnection interruption");
    }).when(connection).reconnect();

    induceReconnect(reconnectController);
    assertNull(replacementController.get());

    assertThatEventually(() -> {
      try {
        otherOperation.join();
        return backgroundInvoked.get();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }, is(true)).within(ofSeconds(30));
  }

  @Test
  public void testNonThrowingOperationWithInterruptedWait() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    /*
     * Create a background task that waits until a reconnection in in progress and
     * attempts to enter a wait for the reconnection to complete.  The wait is interrupted
     * (on purpose) ...
     */
    CyclicBarrier syncPoint = new CyclicBarrier(2);
    CyclicBarrier postReconnectPoint = new CyclicBarrier(2);
    AtomicBoolean backgroundInvoked = new AtomicBoolean();
    AtomicBoolean backgroundInterrupted = new AtomicBoolean();
    AtomicReference<Throwable> backgroundFault = new AtomicReference<>();
    Thread otherOperation = new Thread(() -> {
      try {
        try {
          syncPoint.await();        // Align with reconnect call
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new AssertionError(e);
        }
        Thread.currentThread().interrupt();
        try {
          reconnectController.withConnectionClosedHandling(() -> {
            backgroundInvoked.set(true);
            return null;
          });
          fail("Expecting StoreReconnectInterruptedException");
        } catch (StoreReconnectInterruptedException e) {
          backgroundInterrupted.set(Thread.interrupted());
          backgroundFault.set(e);
          try {
            postReconnectPoint.await();     // Permit reconnect to complete
          } catch (InterruptedException | BrokenBarrierException e1) {
            throw new AssertionError(e1);
          }
        }
      } catch (Throwable e) {
        backgroundFault.set(e);
        postReconnectPoint.reset();
        syncPoint.reset();
      }
    });
    otherOperation.setDaemon(true);

    doAnswer(invocationOnMock -> {
      // This is slightly evil but kick off a background operation to ensure we're blocked
      otherOperation.start();
      syncPoint.await();            // Align with background thread
      postReconnectPoint.await();   // Permit background thread to complete
      return null;
    }).when(connection).reconnect();

    induceReconnect(reconnectController);
    assertNotNull(replacementController.get());

    assertThatEventually(() -> {
      try {
        otherOperation.join();
        return backgroundInvoked.get();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }, is(false)).within(ofSeconds(30));

    assertThat(backgroundFault.get(), is(instanceOf(StoreReconnectInterruptedException.class)));
    assertThat(backgroundFault.get().getCause(), is(instanceOf(InterruptedException.class)));
    assertThat(backgroundFault.get().getSuppressed(), is(matchConnectionClosedException()));
    assertTrue(backgroundInterrupted.get());
  }

  @Test
  public void testNonThrowingOperationConnectionClosedRace() throws Exception {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    /*
     * Both of the following 'withConnectionClosedHandling' tasks will invoke
     * DefaultOperationStrategy.handleReconnect -- one will kick off reconnect and
     * the other will immediately get a StoreOperationAbandonedException.
     */
    CyclicBarrier syncPoint = new CyclicBarrier(2);
    CyclicBarrier reconnectPoint = new CyclicBarrier(2);
    CyclicBarrier postReconnectPoint = new CyclicBarrier(2);
    AtomicReference<Throwable> backgroundFault = new AtomicReference<>();
    Thread otherOperation = new Thread(() -> {
      try {
        try {
          reconnectController.withConnectionClosedHandling(() -> {
            try {
              syncPoint.await();        // Align within ReconnectControllerImpl.DefaultOperationStrategy.execute
              reconnectPoint.await();   // Await other ReconnectableLeasedConnection.reconnect
            } catch (InterruptedException | BrokenBarrierException e) {
              throw new AssertionError(e);
            }
            throw new ConnectionClosedException("raised background");
          });
          fail("Expecting StoreOperationAbandonedException");

        } catch (StoreOperationAbandonedException e) {
          backgroundFault.set(e);
          try {
            postReconnectPoint.await();
          } catch (InterruptedException | BrokenBarrierException e1) {
            throw new AssertionError(e1);
          }
        }

      } catch (Throwable t) {
        backgroundFault.set(t);
        postReconnectPoint.reset();
        reconnectPoint.reset();
        syncPoint.reset();
      }
    });
    otherOperation.setDaemon(true);
    otherOperation.start();

    doAnswer(invocationOnMock -> {
      reconnectPoint.await();       // Continue background thread in ReconnectControllerImpl.DefaultOperationStrategy.execute
      postReconnectPoint.await();   // Permit background thread to complete
      return null;
    }).when(connection).reconnect();

    try {
      reconnectController.withConnectionClosedHandling(() -> {
        try {
          syncPoint.await();    // Align within ReconnectControllerImpl.DefaultOperationStrategy.execute
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new AssertionError(e);
        }
        throw new ConnectionClosedException("raised foreground");
      });
      fail("Expecting StoreOperationAbandonedException");
    } catch (StoreOperationAbandonedException e) {
      // expected
    }

    assertNotNull(replacementController.get());

    assertThatEventually(() -> {
      try {
        otherOperation.join();
        return backgroundFault.get();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }, is(instanceOf(StoreOperationAbandonedException.class))).within(ofSeconds(30));
  }

  /**
   * Confirmation test for TDB-2983.
   */
  @Test
  public void testDisconnectListener() {
    ReconnectControllerImpl reconnectController = new ReconnectControllerImpl(connection);
    AtomicReference<ReconnectController> replacementController = new AtomicReference<>();

    /*
     * First, test the garbage collection behavior ...
     */
    reconnectController.addDisconnectListener(replacementController::set);

    gc();
    induceReconnect(reconnectController);
    assertNull(replacementController.get());

    /*
     * Now retain a reference to the Consumer and try again.
     */
    Consumer<ReconnectController> listener = replacementController::set;
    reconnectController.addDisconnectListener(listener);

    gc();
    induceReconnect(reconnectController);
    assertNotNull(replacementController.get());
  }

  private void induceReconnect(ReconnectControllerImpl reconnectController) {
    try {
      reconnectController.withConnectionClosedHandling(() -> {
        throw new ConnectionClosedException("raised");
      });
      fail("Expecting StoreOperationAbandonedException or StoreReconnectInterruptedException");
    } catch (StoreOperationAbandonedException e) {
      assertThat(e.getCause(), is(instanceOf(ConnectionClosedException.class)));
    } catch (StoreReconnectInterruptedException e) {
      assertThat(e.getSuppressed(), is(matchConnectionClosedException()));
      assertTrue(Thread.interrupted());
    }
  }

  private static void gc() {
    for (int i = 0; i < 5; i++) {
      System.gc();
      System.runFinalization();
      Thread.yield();
    }
  }
}