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

import com.terracottatech.store.coordination.Coordinator.Listener;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.exception.EntityException;
import org.terracotta.exception.EntityNotFoundException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "try"})
public class CoordinationIT {
  @ClassRule
  public static EnterpriseCluster CLUSTER =  newCluster(1).withPlugins(
      "<config>\n" +
      "  <data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n" +
      "    <data:directory name=\"cluster-disk-resource\" use-for-platform=\"true\">ClusterTestDisk</data:directory>\n" +
      "  </data:data-directories>\n" +
      "</config>\n").build();

  private static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @AfterClass
  public static void shutdownExecutor() {
    EXECUTOR.shutdownNow();
  }

  @Test
  public void testSingleClient() throws IOException, ConnectionException {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClient", "alice", EXECUTOR)) {
      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testSingleClient");
  }

  @Test
  public void testSingleClientWithListener() throws IOException, ConnectionException, TimeoutException {
    CollectingListener listener = new CollectingListener();
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientWithListener", "alice", EXECUTOR, listener)) {
      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));
      assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));
    }
    assertThat(listener.events(), contains("joined:alice"));
    assertDestroyed(CLUSTER, "testSingleClientWithListener");
  }

  @Test
  public void testSingleClientClaimingLeadership() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadership", "alice", EXECUTOR)) {
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        assertThat(coordinator.leader().get(), is("alice"));
      }
    }
    assertDestroyed(CLUSTER, "testSingleClientClaimingLeadership");
  }

  @Test
  public void testSingleClientClaimingLeadershipWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    CollectingListener listener = new CollectingListener();
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadershipWithListener", "alice", EXECUTOR, listener)) {
      assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice")).within(Duration.ofSeconds(30));
        assertThat(coordinator.leader().get(), is("alice"));
      }
    }
    assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice", "leadership-relinquished:alice")).within(Duration.ofSeconds(30));
    assertDestroyed(CLUSTER, "testSingleClientClaimingLeadershipWithListener");
  }

  @Test
  public void testSingleClientExecutingTask() throws IOException, ConnectionException, TimeoutException, InterruptedException, ExecutionException, Coordinator.LeadershipViolationException {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientExecutingTask", "alice", EXECUTOR)) {
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        leadership.execute(() -> System.out.println("Hello World"), Duration.ofMinutes(1));
      }
      assertThat(coordinator.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testSingleClientExecutingTask");
  }

  @Test
  public void testTwoClients() throws IOException, ConnectionException {
    try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "alice", EXECUTOR)) {
      assertThat(a.getMembers(), containsInAnyOrder("alice"));
      assertThat(a.leader(), is(empty()));

      try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "bob", EXECUTOR)) {
        assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(b.leader(), is(empty()));

        assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(a.leader(), is(empty()));
      }

      assertThat(a.getMembers(), containsInAnyOrder("alice"));
      assertThat(a.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testTwoClients");
  }

  @Test
  public void testTwoClientsWithCollidingNames() throws IOException, ConnectionException {
    try (Coordinator unused = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "alice", EXECUTOR)) {
      try {
        Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "alice", EXECUTOR).close();
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        //expected
      }
    }
    assertDestroyed(CLUSTER, "testTwoClients");
  }

  @Test
  public void testTwoClientsWithListener() throws IOException, ConnectionException, TimeoutException {
    CollectingListener listenerA = new CollectingListener();
    CollectingListener listenerB = new CollectingListener();

    try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsWithListener", "alice", EXECUTOR, listenerA)) {
      assertThatEventually(listenerA::events, contains("joined:alice")).within(Duration.ofSeconds(30));
      assertThat(a.getMembers(), containsInAnyOrder("alice"));
      assertThat(a.leader(), is(empty()));

      try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsWithListener", "bob", EXECUTOR, listenerB)) {
        assertThatEventually(listenerA::events, containsInAnyOrder("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
        assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(b.leader(), is(empty()));

        assertThatEventually(listenerB::events, containsInAnyOrder("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
        assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(a.leader(), is(empty()));
      }

      assertThatEventually(listenerA::events, containsInAnyOrder("joined:alice", "joined:bob", "left:bob")).within(Duration.ofSeconds(30));
      assertThat(a.getMembers(), containsInAnyOrder("alice"));
      assertThat(a.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testTwoClientsWithListener");
  }

  @Test
  public void testTwoClientsVoluntarilyTradingLeadership() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "alice", EXECUTOR)) {
      try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "bob", EXECUTOR)) {

        try (Coordinator.Leadership al = a.acquireLeadership(Duration.ZERO)) {
          assertThat(a.leader().get(), is("alice"));
          assertThat(b.leader().get(), is("alice"));
          assertThrows(() -> b.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
        }

        try (Coordinator.Leadership bl = b.acquireLeadership(Duration.ZERO)) {
          assertThat(a.leader().get(), is("bob"));
          assertThat(b.leader().get(), is("bob"));
          assertThrows(() -> a.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
        }
      }
    }
    assertDestroyed(CLUSTER, "testTwoClientsVoluntarilyTradingLeadership");
  }

  @Test
  public void testTwoClientsVoluntarilyTradingLeadershipWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    CollectingListener listenerA = new CollectingListener();
    CollectingListener listenerB = new CollectingListener();

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadershipWithListener", "alice", EXECUTOR, listenerA)) {
        assertThatEventually(listenerA::events, contains("joined:alice")).within(Duration.ofSeconds(30));
        try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadershipWithListener", "bob", EXECUTOR, listenerB)) {
          assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
          assertThatEventually(listenerB::events, containsInAnyOrder("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));

          try (Coordinator.Leadership al = a.acquireLeadership(Duration.ZERO)) {
            assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob", "leadership-acquired:alice")).within(Duration.ofSeconds(30));
            assertThatEventually(listenerB::events, containsSequencesInOrder(containsInAnyOrder("joined:alice", "joined:bob"), contains("leadership-acquired:alice"))).within(Duration.ofSeconds(30));

            assertThat(a.leader().get(), is("alice"));
            assertThat(b.leader().get(), is("alice"));
            assertThrows(() -> b.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
          }

          try (Coordinator.Leadership bl = b.acquireLeadership(Duration.ZERO)) {
            assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob", "leadership-acquired:alice", "leadership-relinquished:alice", "leadership-acquired:bob")).within(Duration.ofSeconds(30));
            assertThatEventually(listenerB::events, containsSequencesInOrder(containsInAnyOrder("joined:alice", "joined:bob"), contains("leadership-acquired:alice", "leadership-relinquished:alice", "leadership-acquired:bob"))).within(Duration.ofSeconds(30));
            assertThat(a.leader().get(), is("bob"));
            assertThat(b.leader().get(), is("bob"));
            assertThrows(() -> a.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
          }
        }
      }
    } finally {
      executor.shutdown();
    }
    assertDestroyed(CLUSTER, "testTwoClientsVoluntarilyTradingLeadershipWithListener");
  }

  public static void assertDestroyed(EnterpriseCluster cluster, String token) throws IOException {
    try (Connection connection = cluster.newConnection()) {
      if (connection.getEntityRef(CoordinationEntity.class, 1L, token).destroy()) {
        throw new AssertionError("Entity " + token + " existed");
      } else {
        throw new AssertionError("Entity " + token + " is still in use!");
      }
    } catch (EntityNotFoundException e) {
      //expected
    } catch (EntityException | ConnectionException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("varargs")
  public static <T> Matcher<Iterable<? extends T>> containsSequencesInOrder(Matcher<Iterable<? extends T>>... sequences) {
    if (sequences.length == 0) {
      return emptyIterable();
    } else if (sequences.length == 1) {
      return sequences[0];
    } else {
      Matcher<Iterable<? extends T>> headMatcher = sequences[0];
      Matcher<Iterable<? extends T>> tailMatcher = containsSequencesInOrder(Arrays.copyOfRange(sequences, 1, sequences.length));

      return new TypeSafeMatcher<Iterable<? extends T>>() {
        @Override
        protected boolean matchesSafely(Iterable<? extends T> item) {
          for (int i = 0; ; i++) {
            Iterable<? extends T>[] iterables = splitIterable(item, i);
            if (headMatcher.matches(iterables[0]) && tailMatcher.matches(iterables[1])) {
              return true;
            } else if (!iterables[1].iterator().hasNext()) {
              return false;
            }
          }
        }

        @Override
        public void describeTo(Description description) {
          description.appendDescriptionOf(headMatcher).appendText(", followed-by ").appendDescriptionOf(tailMatcher);
        }
      };
    }
  }

  private static <T> Iterable<T>[] splitIterable(Iterable<T> iterable, int headLength) {
    Iterable<T> it1 = () -> StreamSupport.stream(iterable.spliterator(), false).limit(headLength).iterator();
    Iterable<T> it2 = () -> StreamSupport.stream(iterable.spliterator(), false).skip(headLength).iterator();
    return (Iterable<T>[]) new Iterable<?>[] {it1, it2};
  }

  public static <T extends Throwable> void assertThrows(Task<T, ?> task, Matcher<? super T> throwable) {
    try {
      task.execute();
    } catch (Throwable t) {
      assertThat((T) t, throwable);
      return;
    }
    assertThat(null, throwable);
  }

  interface Task<T extends Throwable, R> {

    R execute() throws T;
  }

  static class CollectingListener implements Listener {

    private final List<String> events = new ArrayList<>();

    @Override
    public synchronized void memberJoined(String identity) {
      events.add("joined:" + identity);
    }

    @Override
    public synchronized void memberLeft(String identity) {
      events.add("left:" + identity);
    }

    @Override
    public synchronized void leadershipAcquiredBy(String identity) {
      events.add("leadership-acquired:" + identity);
    }

    @Override
    public synchronized void leadershipRelinquishedBy(String identity) {
      events.add("leadership-relinquished:" + identity);
    }

    public List<String> events() {
      return unmodifiableList(events);
    }
  }
}
