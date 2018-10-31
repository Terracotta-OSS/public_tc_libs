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

import com.terracottatech.store.coordination.CoordinationIT.CollectingListener;
import com.terracottatech.store.coordination.Coordinator.LeadershipViolationException;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.store.coordination.CoordinationIT.assertDestroyed;
import static com.terracottatech.store.coordination.CoordinationIT.assertThrows;
import static com.terracottatech.store.coordination.CoordinationIT.containsSequencesInOrder;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.Optional.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

@SuppressWarnings("try")
public class CoordinationFailoverIT {
  @ClassRule
  public static EnterpriseCluster CLUSTER =  newCluster(2).withPlugins(
      "<config>\n" +
      "  <data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n" +
      "    <data:directory name=\"cluster-disk-resource\" use-for-platform=\"true\">ClusterTestDisk</data:directory>\n" +
      "  </data:data-directories>\n" +
      "</config>\n").build();

  private static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  @AfterClass
  public static void shutdownExecutor() {
    EXECUTOR.shutdownNow();
  }

  @Before
  public void resetCluster() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @Test
  public void testSingleClient() throws Exception {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClient", "alice", EXECUTOR)) {
      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));

      CLUSTER.getClusterControl().terminateActive();

      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testSingleClient");
  }

  @Test
  public void testSingleClientWithListener() throws Exception {
    CollectingListener listener = new CollectingListener();
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientWithListener", "alice", EXECUTOR, listener)) {
      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));
      assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));

      CLUSTER.getClusterControl().terminateActive();

      assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
      assertThat(coordinator.leader(), is(empty()));
      assertThat(listener.events(), contains("joined:alice"));
    }
    assertThat(listener.events(), contains("joined:alice"));
    assertDestroyed(CLUSTER, "testSingleClientWithListener");
  }

  @Test
  @Ignore("TDB-3839")
  public void testSingleClientClaimingLeadership() throws Exception {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadership", "alice", EXECUTOR)) {
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        assertThat(coordinator.leader().get(), is("alice"));

        CLUSTER.getClusterControl().terminateActive();

        assertThat(coordinator.leader().get(), is("alice"));
      }
    }
    assertDestroyed(CLUSTER, "testSingleClientClaimingLeadership");
  }

  @Test
  public void testSingleClientClaimingLeadershipWithListener() throws Exception {
    CollectingListener listener = new CollectingListener();

    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadershipWithListener", "alice", EXECUTOR, listener)) {
      assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice")).within(Duration.ofSeconds(30));
        assertThat(coordinator.leader().get(), is("alice"));

        CLUSTER.getClusterControl().terminateActive();

        assertThat(listener.events(), contains("joined:alice", "leadership-acquired:alice"));
        assertThat(coordinator.leader().get(), is("alice"));
      }
    }
    assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice", "leadership-relinquished:alice")).within(Duration.ofSeconds(30));
    assertDestroyed(CLUSTER, "testSingleClientClaimingLeadershipWithListener");
  }

  @Test
  public void testSingleClientExecutingTask() throws Exception {
    try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientExecutingTask", "alice", EXECUTOR)) {
      try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
          Future<?> result = executorService.submit(() -> {
            try {
              leadership.execute(() -> {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  throw new AssertionError(e);
                }
              }, Duration.ofMinutes(1));
            } catch (ExecutionException | LeadershipViolationException e) {
              throw new AssertionError(e);
            }
          });

          CLUSTER.getClusterControl().terminateActive();

          result.get(1, TimeUnit.MINUTES);
        } finally {
          executorService.shutdown();
        }

      }
      assertThat(coordinator.leader(), is(empty()));
    }
    assertDestroyed(CLUSTER, "testSingleClientExecutingTask");
  }

  @Test
  public void testTwoClients() throws Exception {
    try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "alice", EXECUTOR)) {
      assertThat(a.getMembers(), containsInAnyOrder("alice"));
      assertThat(a.leader(), is(empty()));

      try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "bob", EXECUTOR)) {
        assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(b.leader(), is(empty()));
        assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(a.leader(), is(empty()));

        CLUSTER.getClusterControl().terminateActive();

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
  public void testTwoClientsWithListener() throws Exception {
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

        CLUSTER.getClusterControl().terminateActive();

        assertThat(listenerA.events(), containsInAnyOrder("joined:alice", "joined:bob"));
        assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
        assertThat(b.leader(), is(empty()));

        assertThat(listenerB.events(), containsInAnyOrder("joined:alice", "joined:bob"));
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
  public void testTwoClientsVoluntarilyTradingLeadership() throws Exception {
    try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "alice", EXECUTOR)) {
      try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "bob", EXECUTOR)) {

        try (Coordinator.Leadership al = a.acquireLeadership(Duration.ZERO)) {
          assertThat(a.leader().get(), is("alice"));
          assertThat(b.leader().get(), is("alice"));
          assertThrows(() -> b.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));

          CLUSTER.getClusterControl().terminateActive();
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

  @SuppressWarnings("unchecked")
  @Test
  public void testTwoClientsVoluntarilyTradingLeadershipWithListener() throws Exception {
    CollectingListener listenerA = new CollectingListener();
    CollectingListener listenerB = new CollectingListener();

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

          CLUSTER.getClusterControl().terminateActive();
        }

        try (Coordinator.Leadership bl = b.acquireLeadership(Duration.ZERO)) {
          assertThatEventually(listenerA::events, containsSequencesInOrder(
              contains("joined:alice", "joined:bob", "leadership-acquired:alice", "leadership-relinquished:alice"),
              either(contains("leadership-acquired:bob"))
                  .or(contains("left:bob", "joined:bob", "leadership-acquired:bob")))
          ).within(Duration.ofSeconds(30));

          assertThatEventually(listenerB::events, containsSequencesInOrder(
              containsInAnyOrder("joined:alice", "joined:bob"),
              contains("leadership-acquired:alice", "leadership-relinquished:alice"),
              either(contains("left:alice", "joined:alice", "leadership-acquired:bob"))
                  .or(contains("leadership-acquired:bob")))
          ).within(Duration.ofSeconds(30));

          assertThat(a.leader().get(), is("bob"));
          assertThat(b.leader().get(), is("bob"));
          assertThrows(() -> a.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
        }
      }
    }
    assertDestroyed(CLUSTER, "testTwoClientsVoluntarilyTradingLeadershipWithListener");
  }
}
