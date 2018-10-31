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

import com.terracottatech.testing.delay.ClusterDelay;
import com.terracottatech.testing.delay.DelayConnectionService;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.connection.ConnectionException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.store.coordination.CoordinationIT.assertThrows;
import static com.terracottatech.store.coordination.CoordinationIT.containsSequencesInOrder;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.util.Optional.empty;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

@SuppressWarnings("try")
public class CoordinationReconnectIT {
  @ClassRule
  public static EnterpriseCluster CLUSTER =  newCluster(1).withPlugins(
          "<config>\n" +
          "  <data:data-directories xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n" +
          "    <data:directory name=\"cluster-disk-resource\" use-for-platform=\"true\">ClusterTestDisk</data:directory>\n" +
          "  </data:data-directories>\n" +
          "</config>\n" +
          "<service xmlns:lease='http://www.terracotta.org/service/lease'>\n" +
          "<lease:connection-leasing>\n " +
          "<lease:lease-length unit='seconds'>5</lease:lease-length>" +
          "</lease:connection-leasing>" +
          "</service>").build();

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
  public void testSingleClient() throws IOException, ConnectionException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClient", "alice", EXECUTOR)) {
        assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
        assertThat(coordinator.leader(), is(empty()));

        clusterDelay.setDefaultMessageDelay(6000);
        Thread.sleep(10000);
        clusterDelay.setDefaultMessageDelay(0);

        assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
        assertThat(coordinator.leader(), is(empty()));
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testSingleClientWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      CoordinationIT.CollectingListener listener = new CoordinationIT.CollectingListener();

      try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientWithListener", "alice", EXECUTOR, listener)) {
        assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
        assertThat(coordinator.leader(), is(empty()));
        assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));

        clusterDelay.setDefaultMessageDelay(6000);
        Thread.sleep(10000);
        clusterDelay.setDefaultMessageDelay(0);

        assertThat(coordinator.getMembers(), containsInAnyOrder("alice"));
        assertThat(coordinator.leader(), is(empty()));
        assertThat(listener.events(), contains("joined:alice"));
      }
      assertThat(listener.events(), contains("joined:alice"));
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testSingleClientClaimingLeadership() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadership", "alice", EXECUTOR)) {
        try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
          assertThat(coordinator.leader().get(), is("alice"));

          clusterDelay.setDefaultMessageDelay(6000);
          Thread.sleep(10000);
          clusterDelay.setDefaultMessageDelay(0);

          assertThat(coordinator.leader().get(), is("alice"));
        }
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testSingleClientClaimingLeadershipWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      CoordinationIT.CollectingListener listener = new CoordinationIT.CollectingListener();

      try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientClaimingLeadershipWithListener", "alice", EXECUTOR, listener)) {
        assertThatEventually(listener::events, contains("joined:alice")).within(Duration.ofSeconds(30));
        try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
          assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice")).within(Duration.ofSeconds(30));
          assertThat(coordinator.leader().get(), is("alice"));

          clusterDelay.setDefaultMessageDelay(6000);
          Thread.sleep(10000);
          clusterDelay.setDefaultMessageDelay(0);

          assertThat(listener.events(), contains("joined:alice", "leadership-acquired:alice"));
          assertThat(coordinator.leader().get(), is("alice"));
        }
      }
      assertThatEventually(listener::events, contains("joined:alice", "leadership-acquired:alice", "leadership-relinquished:alice")).within(Duration.ofSeconds(30));
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testSingleClientExecutingTask() throws IOException, ConnectionException, TimeoutException, InterruptedException, ExecutionException, Coordinator.LeadershipViolationException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      try (Coordinator coordinator = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testSingleClientExecutingTask", "alice", EXECUTOR)) {
        try (Coordinator.Leadership leadership = coordinator.acquireLeadership(Duration.ZERO)) {
          leadership.execute(() -> {
            clusterDelay.setDefaultMessageDelay(6000);
            try {
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
            clusterDelay.setDefaultMessageDelay(0);
          }, Duration.ofMinutes(1));
        }
        assertThat(coordinator.leader(), is(empty()));
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testTwoClients() throws IOException, ConnectionException, InterruptedException, TimeoutException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "alice", EXECUTOR)) {
        assertThat(a.getMembers(), containsInAnyOrder("alice"));
        assertThat(a.leader(), is(empty()));

        try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClients", "bob", EXECUTOR)) {
          assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(b.leader(), is(empty()));

          assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(a.leader(), is(empty()));

          clusterDelay.setDefaultMessageDelay(6000);
          Thread.sleep(10000);
          clusterDelay.setDefaultMessageDelay(0);

          assertThatEventually(b::getMembers, containsInAnyOrder("alice", "bob")).within(Duration.ofSeconds(30));
          assertThat(b.leader(), is(empty()));

          assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(a.leader(), is(empty()));
        }

        assertThat(a.getMembers(), containsInAnyOrder("alice"));
        assertThat(a.leader(), is(empty()));
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTwoClientsWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      CoordinationIT.CollectingListener listenerA = new CoordinationIT.CollectingListener();
      CoordinationIT.CollectingListener listenerB = new CoordinationIT.CollectingListener();

      try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsWithListener", "alice", EXECUTOR, listenerA)) {
        assertThatEventually(listenerA::events, contains("joined:alice")).within(Duration.ofSeconds(30));
        assertThat(a.getMembers(), containsInAnyOrder("alice"));
        assertThat(a.leader(), is(empty()));

        try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsWithListener", "bob", EXECUTOR, listenerB)) {
          assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
          assertThat(b.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(b.leader(), is(empty()));

          assertThatEventually(listenerB::events, containsInAnyOrder("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
          assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(a.leader(), is(empty()));

          clusterDelay.setDefaultMessageDelay(6000);
          Thread.sleep(10000);
          clusterDelay.setDefaultMessageDelay(0);

          assertThatEventually(b::getMembers, containsInAnyOrder("alice", "bob")).within(Duration.ofSeconds(30));
          assertThatEventually(listenerA::events, containsSequencesInOrder(
              contains("joined:alice", "joined:bob"),
              either(contains("left:bob", "joined:bob")).or(emptyIterable()))).within(Duration.ofSeconds(30));
          assertThat(b.leader(), is(empty()));

          assertThatEventually(listenerB::events, containsSequencesInOrder(
              containsInAnyOrder("joined:alice", "joined:bob"),
              either(contains("left:alice", "joined:alice")).or(emptyIterable()))).within(Duration.ofSeconds(30));
          assertThat(a.getMembers(), containsInAnyOrder("alice", "bob"));
          assertThat(a.leader(), is(empty()));
        }

        assertThatEventually(listenerA::events, containsSequencesInOrder(
            contains("joined:alice", "joined:bob"),
            either(contains("left:bob", "joined:bob", "left:bob")).or(contains("left:bob")))
        ).within(Duration.ofSeconds(30));
        assertThat(a.getMembers(), containsInAnyOrder("alice"));
        assertThat(a.leader(), is(empty()));
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @Test
  public void testTwoClientsVoluntarilyTradingLeadership() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "alice", EXECUTOR)) {
        try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadership", "bob", EXECUTOR)) {

          try (Coordinator.Leadership al = a.acquireLeadership(Duration.ZERO)) {
            assertThat(a.leader().get(), is("alice"));
            assertThat(b.leader().get(), is("alice"));
            assertThrows(() -> b.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
          }

          clusterDelay.setDefaultMessageDelay(6000);
          Thread.sleep(10000);
          clusterDelay.setDefaultMessageDelay(0);

          try (Coordinator.Leadership bl = b.acquireLeadership(Duration.ZERO)) {
            assertThat(a.leader().get(), is("bob"));
            assertThat(b.leader().get(), is("bob"));
            assertThrows(() -> a.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
          }
        }
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTwoClientsVoluntarilyTradingLeadershipWithListener() throws IOException, ConnectionException, TimeoutException, InterruptedException {
    ClusterDelay clusterDelay = new ClusterDelay("coordinationDelay", 1);
    DelayConnectionService.setDelay(clusterDelay);
    try {
      CoordinationIT.CollectingListener listenerA = new CoordinationIT.CollectingListener();
      CoordinationIT.CollectingListener listenerB = new CoordinationIT.CollectingListener();

      try (Coordinator a = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadershipWithListener", "alice", EXECUTOR, listenerA)) {
        assertThatEventually(listenerA::events, contains("joined:alice")).within(Duration.ofSeconds(30));
        try (Coordinator b = Coordinator.coordinator(CLUSTER.getConnectionURI(), "testTwoClientsVoluntarilyTradingLeadershipWithListener", "bob", EXECUTOR, listenerB)) {
          assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));
          assertThatEventually(listenerB::events, containsInAnyOrder("joined:alice", "joined:bob")).within(Duration.ofSeconds(30));

          try (Coordinator.Leadership al = a.acquireLeadership(Duration.ZERO)) {
            assertThatEventually(listenerA::events, contains("joined:alice", "joined:bob", "leadership-acquired:alice")).within(Duration.ofSeconds(30));
            assertThatEventually(listenerB::events, containsSequencesInOrder(
                containsInAnyOrder("joined:alice", "joined:bob"),
                contains("leadership-acquired:alice"))
            ).within(Duration.ofSeconds(30));

            assertThat(a.leader().get(), is("alice"));
            assertThat(b.leader().get(), is("alice"));
            assertThrows(() -> b.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));

          }

          try (Coordinator.Leadership bl = b.acquireLeadership(Duration.ZERO)) {

            clusterDelay.setDefaultMessageDelay(6000);
            Thread.sleep(10000);
            clusterDelay.setDefaultMessageDelay(0);

            assertThatEventually(listenerA::events, containsSequencesInOrder(
                contains("joined:alice", "joined:bob", "leadership-acquired:alice", "leadership-relinquished:alice", "leadership-acquired:bob"),
                either(contains("left:bob", "joined:bob")).or(emptyIterable()))
            ).within(Duration.ofSeconds(30));
            assertThatEventually(listenerB::events, containsSequencesInOrder(
                containsInAnyOrder("joined:alice", "joined:bob"),
                contains("leadership-acquired:alice", "leadership-relinquished:alice", "leadership-acquired:bob"),
                either(contains("left:alice", "joined:alice")).or(emptyIterable()))
            ).within(Duration.ofSeconds(30));
            assertThat(a.leader().get(), is("bob"));
            assertThat(b.leader().get(), is("bob"));
            assertThrows(() -> a.acquireLeadership(Duration.ZERO), instanceOf(TimeoutException.class));
          }
        }
      }
    } finally {
      DelayConnectionService.setDelay(null);
    }
  }
}
