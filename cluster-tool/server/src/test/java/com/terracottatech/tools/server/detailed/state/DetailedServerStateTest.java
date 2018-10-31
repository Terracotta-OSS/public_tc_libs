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
package com.terracottatech.tools.server.detailed.state;

import org.junit.Test;

import static com.terracottatech.tools.detailed.state.LogicalServerState.ACTIVE_RECONNECTING;
import static com.terracottatech.tools.detailed.state.LogicalServerState.ACTIVE_SUSPENDED;
import static com.terracottatech.tools.detailed.state.LogicalServerState.UNKNOWN;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DetailedServerStateTest {

  @Test
  public void getDetailedServerState_jmx_down() {
    SubSystemCaller mock = mock(SubSystemCaller.class);
    when(mock.hasConsistencyManager()).thenReturn(false);
    when(mock.isReconnectWindow()).thenReturn("Invalid JMX call");
    when(mock.getState()).thenReturn("Invalid JMX call");

    assertThat(DetailedServerState.retrieveDetailedServerState(mock), equalTo(UNKNOWN));
    verify(mock, never()).isBlocked();
  }

  @Test
  public void getDetailedServerState_reconnect_window() {
    SubSystemCaller mock = mock(SubSystemCaller.class);
    when(mock.isReconnectWindow()).thenReturn("true");
    when(mock.hasConsistencyManager()).thenReturn(true);
    when(mock.isBlocked()).thenReturn("false");
    when(mock.getState()).thenReturn("ACTIVE");

    assertThat(DetailedServerState.retrieveDetailedServerState(mock), equalTo(ACTIVE_RECONNECTING));
    verify(mock).isBlocked();
  }

  @Test
  public void getDetailedServerState_active_is_blocked() {
    SubSystemCaller mock = mock(SubSystemCaller.class);
    when(mock.isReconnectWindow()).thenReturn("false");
    when(mock.hasConsistencyManager()).thenReturn(true);
    when(mock.isBlocked()).thenReturn("true");
    when(mock.getState()).thenReturn("ACTIVE");

    assertThat(DetailedServerState.retrieveDetailedServerState(mock), equalTo(ACTIVE_SUSPENDED));
    verify(mock).isBlocked();
  }

}