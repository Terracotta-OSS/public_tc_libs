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
package com.terracottatech.connection.disconnect;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class RelayDisconnectListenerTest {
  @Mock
  private DisconnectListener listener;

  @Test
  public void justSettingADisconnectListenerDoesNotFireAnEvent() {
    RelayDisconnectListener relayListener = new RelayDisconnectListener();
    relayListener.setDisconnectListener(listener);

    verifyNoMoreInteractions(listener);
  }

  @Test
  public void disconnectedCallsExistingListener() {
    RelayDisconnectListener relayListener = new RelayDisconnectListener();
    relayListener.setDisconnectListener(listener);
    relayListener.disconnected();

    verify(listener).disconnected();
  }

  @Test
  public void aLaterListenerGetsTheDisconnectedCall() {
    RelayDisconnectListener relayListener = new RelayDisconnectListener();
    relayListener.disconnected();
    relayListener.setDisconnectListener(listener);

    verify(listener).disconnected();
  }

  @Test(expected = IllegalStateException.class)
  public void cannotSetAListenerTwice() {
    RelayDisconnectListener relayListener = new RelayDisconnectListener();
    relayListener.setDisconnectListener(listener);
    relayListener.setDisconnectListener(listener);
  }
}
