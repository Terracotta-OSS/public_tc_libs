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
package com.terracottatech.tools.clustertool.managers;

import com.terracotta.diagnostic.Diagnostics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class StatusManagerTest {
  @Mock
  private CommonEntityManager entityManager;

  @Mock
  private DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity connDiagnosticsEntity;

  @Mock
  private Diagnostics diagnostics;

  private final String hostPort = "1.1.1.1";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testActiveSuspendedState() {
    when(connDiagnosticsEntity.getDiagnostics()).thenReturn(diagnostics);
    when(connDiagnosticsEntity.getDiagnostics().invoke("DetailedServerState", "getDetailedServerState")).thenReturn("ACTIVE_SUSPENDED");
    when(entityManager.getDiagnosticsEntity(hostPort, null)).thenReturn(connDiagnosticsEntity);
    assertThat(new StatusManager(entityManager).getServerStatus(hostPort), containsString("ACTIVE_SUSPENDED"));
  }

  @Test
  public void testConnectionFailure() {
    when(connDiagnosticsEntity.getDiagnostics()).thenThrow(new RuntimeException());
    when(entityManager.getDiagnosticsEntity(hostPort, null)).thenReturn(connDiagnosticsEntity);
    assertThat(new StatusManager(entityManager).getServerStatus(hostPort), containsString("UNREACHABLE"));
  }

  @Test
  public void testUnregisteredMBean() {
    when(connDiagnosticsEntity.getDiagnostics()).thenReturn(diagnostics);
    when(connDiagnosticsEntity.getDiagnostics().invoke(anyString(), anyString())).thenReturn("Invalid JMX call");
    when(connDiagnosticsEntity.getDiagnostics().getState()).thenReturn("ACTIVE");
    when(entityManager.getDiagnosticsEntity(hostPort, null)).thenReturn(connDiagnosticsEntity);
    assertThat(new StatusManager(entityManager).getServerStatus(hostPort), containsString("ACTIVE"));
  }
}
