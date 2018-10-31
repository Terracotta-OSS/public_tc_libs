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
package com.terracottatech.tools.util;

import com.terracotta.diagnostic.Diagnostics;
import org.junit.Test;

import static com.terracottatech.tools.detailed.state.LogicalServerState.PASSIVE;
import static com.terracottatech.tools.detailed.state.LogicalServerState.UNKNOWN;
import static com.terracottatech.tools.util.DetailedServerStateUtil.INVALID_JMX_CALL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class DetailedServerStateUtilTest {

  @Test
  public void getDetailedServerStateTest_using_detailedServerMBean() {
    Diagnostics diagnostics =  mock(Diagnostics.class);
    when(diagnostics.invoke("DetailedServerState", "getDetailedServerState")).thenReturn("ACTIVE");

    assertThat(DetailedServerStateUtil.getDetailedServerState(diagnostics), equalTo("ACTIVE"));
  }

  @Test
  public void getDetailedServerStateTest_using_serverState() {
    Diagnostics diagnostics =  mock(Diagnostics.class);
    when(diagnostics.invoke("DetailedServerState", "getDetailedServerState")).thenReturn(INVALID_JMX_CALL);
    when(diagnostics.getState()).thenReturn("PASSIVE");

    assertThat(DetailedServerStateUtil.getDetailedServerState(diagnostics), equalTo(PASSIVE.name()));
  }

  @Test
  public void getDetailedServerStateTest_nothing_worked() {
    Diagnostics diagnostics =  mock(Diagnostics.class);
    when(diagnostics.invoke("DetailedServerState", "getDetailedServerState")).thenReturn(INVALID_JMX_CALL);
    when(diagnostics.getState()).thenReturn(INVALID_JMX_CALL);

    assertThat(DetailedServerStateUtil.getDetailedServerState(diagnostics), equalTo(UNKNOWN.name()));
  }
}