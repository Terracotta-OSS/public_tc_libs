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

package com.terracottatech.tools.clustertool.backup;


import com.terracotta.diagnostic.Diagnostics;
import com.terracottatech.br.common.BackupExecutionRequest;
import com.terracottatech.br.common.BackupExecutionResult;
import com.terracottatech.tools.client.TopologyEntity;
import com.terracottatech.tools.client.TopologyEntityProvider;
import com.terracottatech.tools.clustertool.managers.BackupManager;
import com.terracottatech.tools.clustertool.managers.DefaultBackupManager;
import com.terracottatech.tools.clustertool.managers.CommonEntityManager;
import com.terracottatech.tools.clustertool.managers.TopologyManager;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.DefaultConfigurationParser;
import com.terracottatech.tools.config.Stripe;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import org.terracotta.connection.Connection;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

public class BackupTest {

  private static final String CLUSTER_NAME = "test";

  private final Map<Diagnostics, String> diagnosticsServerMap = new ConcurrentHashMap<>();

  @After
  public void cleanup() {
    diagnosticsServerMap.clear();
  }

  @Test
  public void testSingleStripeSuccessful() {
    AtomicBoolean prepareSuccess = new AtomicBoolean(false);

    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml");
    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_AND_ENTER_ONLINE_BACKUP_MODE.getCmdName())) {
        if (prepareSuccess.compareAndSet(false, true)) {
          return BackupExecutionResult.SUCCESS.name();
        } else {
          return BackupExecutionResult.NOOP.name();
        }
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      fail("Not expecting any exception. Exception: " + e);
    }
  }

  @Test
  public void testSingleStripeFailureInPrepare() {
    AtomicInteger abortCalled = new AtomicInteger();
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml");
    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_AND_ENTER_ONLINE_BACKUP_MODE.getCmdName())) {
        return BackupExecutionResult.PREPARE_TIMEOUT.name();
      }

      if (invocation.getArgument(1).equals(BackupExecutionRequest.ABORT_BACKUP.getCmdName())) {
        abortCalled.incrementAndGet();
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Backup failed as no server enters into backup mode"));
      assertThat(abortCalled.get(), is(topologyEntity.getClusterConfiguration().getCluster().getStripes().get(0).getServers().size()));
    }
  }

  @Test
  public void testSingleStripeFailureInBackup() {
    testSingleStripeFailure(BackupExecutionRequest.START_BACKUP.getCmdName(), BackupExecutionResult.BACKUP_FAILURE.name());
  }

  @Test
  public void testSingleStripeFailureInExit() {
    testSingleStripeFailure(BackupExecutionRequest.EXIT_ONLINE_BACKUP_MODE.getCmdName(), BackupExecutionResult.BACKUP_FAILURE.name());
  }

  @Test
  public void testSingleStripeFailureUnknown() {
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml");
    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_AND_ENTER_ONLINE_BACKUP_MODE.getCmdName())) {
        return BackupExecutionResult.SUCCESS.name();
      }

      if (invocation.getArgument(1).equals(BackupExecutionRequest.START_BACKUP.getCmdName())) {
        return "Request Timeout";
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("Aborting backup as the connection timed out during the backup process."));
    }
  }

  @Test
  public void testMultiStripeSuccessful() {
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml", "tc-config2.xml");
    Map<Stripe, AtomicBoolean> stripePrepareSuccessMap = new HashMap<>();
    Map<String, Stripe> serverNameStripeMap = new HashMap<>();
    topologyEntity.getClusterConfiguration().getCluster().getStripes().forEach(stripe -> {
      stripePrepareSuccessMap.put(stripe, new AtomicBoolean());
      stripe.getServers().forEach(server -> serverNameStripeMap.put(server.getHostPort(), stripe));
    });

    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_FOR_BACKUP.getCmdName())) {
        AtomicBoolean prepareSuccess = stripePrepareSuccessMap.get(serverNameStripeMap.get(diagnosticsServerMap.get(invocation.getMock())));
        if (prepareSuccess.compareAndSet(false, true)) {
          return BackupExecutionResult.SUCCESS.name();
        } else {
          return BackupExecutionResult.NOOP.name();
        }
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      fail("Not expecting any exception. Exception: " + e);
    }
  }

  @Test
  public void testMultiStripeFailureWhileSettingBackupName() {
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml", "tc-config2.xml");
    CommonEntityManager entityManager = mock(CommonEntityManager.class);
    when(entityManager.getDiagnosticsEntity(anyString(), anyString())).thenAnswer(invocation -> {
      Diagnostics diagnostics = mock(Diagnostics.class);
      when(diagnostics.getState()).thenReturn("PASSIVE");
      when(diagnostics.set(eq(DefaultBackupManager.DIAGNOSTIC_NAME), anyString(), anyString())).thenReturn("FAILURE");
      return new ConnectionCloseableDiagnosticsEntity(diagnostics, mock(Connection.class));
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Unable to start backup"));
    }
  }

  @Test
  public void testMultiStripeFailureInPrepare() {
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml", "tc-config2.xml");
    Map<Stripe, AtomicBoolean> stripePrepareSuccessMap = new HashMap<>();
    Map<String, Stripe> serverNameStripeMap = new HashMap<>();
    topologyEntity.getClusterConfiguration().getCluster().getStripes().forEach(stripe -> {
      stripePrepareSuccessMap.put(stripe, new AtomicBoolean());
      stripe.getServers().forEach(server -> serverNameStripeMap.put(server.getHostPort(), stripe));
    });

    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_FOR_BACKUP.getCmdName())) {
        AtomicBoolean prepareSuccess = stripePrepareSuccessMap.get(serverNameStripeMap.get(diagnosticsServerMap.get(invocation.getMock())));
        if (prepareSuccess.compareAndSet(false, true)) {
          return BackupExecutionResult.PREPARE_TIMEOUT.name();
        } else {
          return BackupExecutionResult.NOOP.name();
        }
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Backup failed"));
    }
  }

  @Test
  public void testMultiStripeFailureInEnterBackupMode() {
    testMultiStripeFailure(BackupExecutionRequest.ENTER_ONLINE_BACKUP_MODE);
  }

  @Test
  public void testMultiStripeFailureInStartBackup() {
    testMultiStripeFailure(BackupExecutionRequest.START_BACKUP);
  }

  @Test
  public void testMultiStripeFailureInExitBackup() {
    testMultiStripeFailure(BackupExecutionRequest.EXIT_ONLINE_BACKUP_MODE);
  }

  private void testSingleStripeFailure(String backupExecutionRequest, String backupExecutionResult) {
    AtomicInteger abortCalled = new AtomicInteger();
    AtomicBoolean prepareSuccess = new AtomicBoolean(false);

    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml");
    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_AND_ENTER_ONLINE_BACKUP_MODE.getCmdName())) {
        if (prepareSuccess.compareAndSet(false, true)) {
          return BackupExecutionResult.SUCCESS.name();
        } else {
          return BackupExecutionResult.NOOP.name();
        }
      }

      if (invocation.getArgument(1).equals(backupExecutionRequest)) {
        return backupExecutionResult;
      }

      if (invocation.getArgument(1).equals(BackupExecutionRequest.ABORT_BACKUP.getCmdName())) {
        abortCalled.incrementAndGet();
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Backup failed"));
      assertThat(abortCalled.get(), is(topologyEntity.getClusterConfiguration().getCluster().getStripes().get(0).getServers().size()));
    }
  }

  private void testMultiStripeFailure(BackupExecutionRequest backupExecutionRequest) {
    AtomicInteger abortCalled = new AtomicInteger();
    TopologyEntity topologyEntity = mockedTopologyEntity("tc-config1.xml", "tc-config2.xml");
    Map<Stripe, AtomicBoolean> stripePrepareSuccessMap = new HashMap<>();
    Map<String, Stripe> serverNameStripeMap = new HashMap<>();
    topologyEntity.getClusterConfiguration().getCluster().getStripes().forEach(stripe -> {
      stripePrepareSuccessMap.put(stripe, new AtomicBoolean());
      stripe.getServers().forEach(server -> serverNameStripeMap.put(server.getHostPort(), stripe));
    });

    CommonEntityManager entityManager = mockedEntityManager(invocation -> {
      if (invocation.getArgument(1).equals(BackupExecutionRequest.PREPARE_FOR_BACKUP.getCmdName())) {
        AtomicBoolean prepareSuccess = stripePrepareSuccessMap.get(serverNameStripeMap.get(diagnosticsServerMap.get(invocation.getMock())));
        if (prepareSuccess.compareAndSet(false, true)) {
          return BackupExecutionResult.SUCCESS.name();
        } else {
          return BackupExecutionResult.NOOP.name();
        }
      }

      if (invocation.getArgument(1).equals(backupExecutionRequest.getCmdName())) {
        return BackupExecutionResult.BACKUP_FAILURE.name();
      }

      if (invocation.getArgument(1).equals(BackupExecutionRequest.ABORT_BACKUP.getCmdName())) {
        abortCalled.incrementAndGet();
      }

      return BackupExecutionResult.SUCCESS.name();
    });

    try {
      mockedBackupManager(topologyEntity, entityManager).backup(CLUSTER_NAME, topologyEntity.getClusterConfiguration().getCluster().hostPortList());
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Backup failed"));
      assertThat(abortCalled.get(), is(topologyEntity.getClusterConfiguration().getCluster().getServers().size()));
    }
  }

  private BackupManager mockedBackupManager(TopologyEntity topologyEntity, CommonEntityManager entityManager) {
    String clusterName = topologyEntity.getClusterConfiguration().getClusterName();
    ClusterConfiguration clusterConfiguration = topologyEntity.getClusterConfiguration();
    TopologyManager topologyManager = mock(TopologyManager.class);
    when(entityManager.getTopologyEntity(ArgumentMatchers.<String>anyList(), anyString()))
        .thenReturn(new TopologyEntityProvider.ConnectionCloseableTopologyEntity(topologyEntity, mock(Connection.class)));
    when(topologyManager.getSecurityRootDirectory()).thenReturn("blah");
    when(topologyManager.validateAndGetClusterConfiguration(topologyEntity, clusterName)).thenReturn(clusterConfiguration);
    return new DefaultBackupManager(topologyManager, entityManager);
  }

  private TopologyEntity mockedTopologyEntity(String... resourceConfigNames) {
    String[] configs = Arrays.stream(resourceConfigNames).map(this::readContent).toArray(String[]::new);
    ClusterConfiguration clusterConfiguration = new ClusterConfiguration(CLUSTER_NAME, new DefaultConfigurationParser().parseConfigurations(configs));
    TopologyEntity topologyEntity = mock(TopologyEntity.class);
    when(topologyEntity.getClusterConfiguration()).thenReturn(clusterConfiguration);
    return topologyEntity;
  }

  private CommonEntityManager mockedEntityManager(Answer<?> answer) {
    CommonEntityManager entityManager = mock(CommonEntityManager.class);
    when(entityManager.getDiagnosticsEntity(anyString(), anyString())).thenAnswer(invocation -> {
      Diagnostics diagnostics = mock(Diagnostics.class);
      when(diagnostics.getState()).thenReturn("PASSIVE");
      when(diagnostics.set(eq(DefaultBackupManager.DIAGNOSTIC_NAME), anyString(), anyString())).thenReturn("SUCCESS");
      when(diagnostics.invoke(eq(DefaultBackupManager.DIAGNOSTIC_NAME), anyString())).thenAnswer(answer);
      diagnosticsServerMap.put(diagnostics, invocation.getArgument(0).toString());
      return new ConnectionCloseableDiagnosticsEntity(diagnostics, mock(Connection.class));
    });
    return entityManager;
  }

  private String readContent(String resourceConfigName) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream inputStream = BackupTest.class.getResourceAsStream("/" + resourceConfigName);
      byte[] buffer = new byte[1024];
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, length);
      }
      return baos.toString("UTF-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
