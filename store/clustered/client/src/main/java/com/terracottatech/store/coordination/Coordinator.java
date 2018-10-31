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
package com.terracottatech.store.coordination;

import org.terracotta.connection.ConnectionException;
import org.terracotta.lease.connection.LeasedConnectionFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.callable;

/**
 * Simple tooling for coordination across a Terracotta cluster.
 */
public interface Coordinator extends Closeable {

  /**
   * Create a coordinator instance for the given cluster and token, with an associated listener.
   * <p>
   * The supplied executor is used to fire the listener events and to asynchronously reconnect to the cluster.
   *
   * @param cluster the URI for the cluster
   * @param token coordinator identifier; this is used to "scope" the coordinator within the cluster and is
   *              generally associated with the resource for which the coordinator is used
   * @param name member name; this should uniquely identify a coordination participant within {@code token}
   * @param listener coordinator event listener
   * @param executor listener and reconnect worker
   * @return a cluster coordinator instance
   * @throws IllegalArgumentException if the name is already in use
   * @throws NullPointerException if any of the parameters are null
   * @throws ConnectionException if unable to obtain a connection to the {@code cluster}
   */
  static Coordinator coordinator(URI cluster, String token, String name, Executor executor, Listener listener)
      throws IllegalArgumentException, NullPointerException, ConnectionException {
    requireNonNull(cluster, "Cluster URI must not be null");
    return new CoordinatorImpl(() -> LeasedConnectionFactory.connect(cluster, new Properties()),
        requireNonNull(token, "Coordination token must not be null"),
        requireNonNull(name, "Member name must not be null"),
        requireNonNull(executor, "Executor must not be null"),
        requireNonNull(listener, "Listener must not be null"));
  }

  /**
   * Create a coordinator instance for the given cluster and token, with an associated listener.
   * <p>
   * The supplied executor is used to fire the listener events and to asynchronously reconnect to the cluster.
   *
   * @param servers the list of servers forming the cluster
   * @param token coordinator identifier; this is used to "scope" the coordinator within the cluster and is
   *              generally associated with the resource for which the coordinator is used
   * @param name member name; this should uniquely identify a coordination participant within {@code token}
   * @param listener coordinator event listener
   * @param executor listener and reconnect worker
   * @return a cluster coordinator instance
   * @throws IllegalArgumentException if the name is already in use
   * @throws NullPointerException if any of the parameters are null
   * @throws ConnectionException if unable to obtain a connection to the {@code servers}
   */
  static Coordinator coordinator(Iterable<InetSocketAddress> servers, String token, String name, Executor executor, Listener listener)
      throws IllegalArgumentException, NullPointerException, ConnectionException {
    requireNonNull(servers, "Server list must not be null");
    return new CoordinatorImpl(
        () -> LeasedConnectionFactory.connect(servers, new Properties()),
        requireNonNull(token, "Coordination token must not be null"),
        requireNonNull(name, "Member name must not be null"),
        requireNonNull(executor, "Executor must not be null"),
        requireNonNull(listener, "Listener must not be null"));
  }

  /**
   * Create a coordinator instance for the given cluster and token.
   * <p>
   * The supplied executor is used to asynchronously reconnect to the cluster.
   *
   * @param cluster the URI for the cluster
   * @param token coordinator identifier; this is used to "scope" the coordinator within the cluster and is
   *              generally associated with the resource for which the coordinator is used
   * @param name member name; this should uniquely identify a coordination participant within {@code token}
   * @param executor reconnect worker
   * @return a cluster coordinator instance
   * @throws IllegalArgumentException if the name is already in use
   * @throws NullPointerException if any of the parameters are null
   * @throws ConnectionException if unable to obtain a connection to the {@code cluster}
   */
  static Coordinator coordinator(URI cluster, String token, String name, Executor executor)
      throws IllegalArgumentException, NullPointerException, ConnectionException {
    requireNonNull(cluster, "Cluster URI must not be null");
    return new CoordinatorImpl(() -> LeasedConnectionFactory.connect(cluster, new Properties()),
        requireNonNull(token, "Coordination token must not be null"),
        requireNonNull(name, "Member name must not be null"),
        requireNonNull(executor, "Executor must not be null"),
        null);
  }

  /**
   * Create a coordinator instance for the given cluster and token.
   * <p>
   * The supplied executor is used to asynchronously reconnect to the cluster.
   *
   * @param servers the list of servers forming the cluster
   * @param token coordinator identifier; this is used to "scope" the coordinator within the cluster and is
   *              generally associated with the resource for which the coordinator is used
   * @param name member name; this should uniquely identify a coordination participant within {@code token}
   * @param executor reconnect worker
   * @return a cluster coordinator instance
   * @throws IllegalArgumentException if the name is already in use
   * @throws NullPointerException if any of the parameters are null
   * @throws ConnectionException if unable to obtain a connection to the {@code servers}
   */
  static Coordinator coordinator(Iterable<InetSocketAddress> servers, String token, String name, Executor executor)
      throws IllegalArgumentException, NullPointerException, ConnectionException {
    requireNonNull(servers, "Server list must not be null");
    return new CoordinatorImpl(() -> LeasedConnectionFactory.connect(servers, new Properties()),
        requireNonNull(token, "Coordination token must not be null"),
        requireNonNull(name, "Member name must not be null"),
        requireNonNull(executor, "Executor must not be null"),
        null);
  }

  /**
   * Return the current set of members in this coordinator.
   *
   * @return the coordinator members
   */
  Set<String> getMembers();

  /**
   * Return the name of the current leader.
   *
   * @return the current leader
   */
  Optional<String> leader();

  /**
   * Try to acquire leadership of the given token.
   * <p>
   *   If leadership is acquired then tasks can be run as the leader using the returned leadership object.
   *   If leadership cannot be acquired then an empty {@code Optional} will be returned.
   * </p>
   *
   * @return a handle representing leadership of the token, or an empty {@code Optional}
   */
  Optional<Leadership> tryAcquireLeadership();

  /**
   * Try to acquire leadership of the given token within the supplied time.
   * <p>
   *   If leadership is acquired then tasks can be run as the leader using the returned leadership object.
   *   If leadership cannot be acquired within the given time than a {@code TimeoutException} is thrown.
   * </p>
   *
   * @param timeout time available to acquire leadership; if {@link Duration#ZERO}, acquires leadership
   *                if, and only if, leadership is immediately available
   * @return a handle representing leadership of the token
   * @throws TimeoutException if leadership cannot be acquired before timeout
   * @throws InterruptedException if the thread is interrupted on entry or while acquiring leadership
   */
  Leadership acquireLeadership(Duration timeout) throws TimeoutException, InterruptedException;

  /**
   * A representation of a leadership role.
   * <p>
   * Leadership, once acquired, is retained until:
   * <ol>
   *   <li>explicitly relinquished by calling {@link Coordinator.Leadership#close Leadership.close}</li>
   *   <li>another member takes leadership by calling {@link #tryAcquireLeadership} or {@link #acquireLeadership}
   *   while this client is disconnected</li>
   * </ol>
   */
  interface Leadership extends AutoCloseable {

    /**
     * Execute the given callable as the rightful leader.
     * <p>
     *   If the leadership is no longer validly held when invoking this method then an {@code LeadershipViolationException}
     *   will be thrown prior to executing the callable.  If the leadership is validly held then the leadership will be
     *   leased for the supplied time while the callable is executed.  Upon completion of the callable the server
     *   will be contacted to release the leadership lease. If upon reaching the server the leadership has been lost
     *   then the operation will throw an {@code LeadershipViolationException}.
     * </p>
     *
     * @param callable leadership task to perform
     * @param duration leadership lease time
     * @param <R> callable return type
     * @return the result of the callable
     * @throws ExecutionException wrapping any throwable from the callable
     * @throws LeadershipViolationException if leadership is not held or is lost during execution
     */
    <R> R execute(Callable<R> callable, Duration duration) throws LeadershipViolationException, ExecutionException;

    /**
     * Execute the given runnable as the rightful leader.
     *
     * @param runnable leadership task to perform
     * @param duration leadership lease time
     * @throws ExecutionException wrapping any throwable from the runnable
     * @throws LeadershipViolationException if leadership is not held or is lost during execution
     * @see #execute(Callable, Duration)
     */
    default void execute(Runnable runnable, Duration duration) throws LeadershipViolationException, ExecutionException {
      execute(callable(runnable), duration);
    }

    /**
     * Voluntarily relinquish leadership.
     */
    @Override
    void close();
  }

  /**
   * Coordinator event listener interface
   */
  interface Listener {

    /**
     * A member with the given {@code name} joined the coordinator.
     *
     * @param name joining members name
     */
    default void memberJoined(String name) {}

    /**
     * A member with the given {@code name} left the coordinator.
     *
     * @param name leaving members name
     */
    default void memberLeft(String name) {}

    /**
     * The given member acquired leadership of the coordinator.
     *
     * @param name leaders name
     */
    default void leadershipAcquiredBy(String name) {}

    /**
     * The given member lost leadership of the coordinator.
     *
     * @param name prior leaders name
     */
    default void leadershipRelinquishedBy(String name) {}
  }

  /**
   * Thrown to indicate a leadership violation while attempting to execute a task.
   * <p>
   * If the associated task:
   * <ul>
   *   <li>has not been executed then the {@link #getResult()} method will throw {@code IllegalStateException}.</li>
   *   <li>terminated normally then the {@code getResult()} method will return the result of the task.</li>
   *   <li>terminated exceptionally then the {@code getResult()} method will throw an {@code ExecutionException}
   *   wrapping the original failure. The same exception will also be attached as a supressed exception of this
   *   {@code LeadershipViolationException}.</li>
   * </ul>
   */
  class LeadershipViolationException extends Exception {

    private static final long serialVersionUID = 5483465745654150985L;

    private final Object result;
    private final ExecutionException failure;

    /**
     * Constructs a pre-execution leadership violation.
     *
     * @param message exception message
     */
    LeadershipViolationException(String message) {
      super(message);
      this.result = null;
      this.failure = null;
    }

    /**
     * Constructs a post-execution leadership violation after a successful task execution.
     *
     * @param message exception message
     */
    LeadershipViolationException(String message, Object result) {
      super(message);
      this.result = result;
      this.failure = null;
    }

    /**
     * Constructs a post-execution leadership violation after a successful task execution.
     *
     * @param message exception message
     */
    LeadershipViolationException(String message, ExecutionException failure) {
      super(message);
      this.result = null;
      this.failure = failure;
      this.addSuppressed(failure);
    }

    /**
     * Return the result of the associated leadership task execution.
     * <p>
     *   If the associated task completed normally then the result will be returned. If the the task completed
     *   exceptionally then an {@code ExecutionException} will be thrown wrapping the exception thrown by the task.
     * </p>
     * @param <R> unbound result parameter
     * @return the result of associated leadership task
     * @throws ExecutionException if the associated task failed
     * @throws IllegalStateException if the associated task was not executed
     */
    public <R> R getResult() throws ExecutionException, IllegalStateException {
      if (result != null) {
        @SuppressWarnings("unchecked")
        R result = (R) this.result;
        return result;
      } else if (failure != null) {
        throw failure;
      } else {
        throw new IllegalStateException();
      }
    }
  }

}
