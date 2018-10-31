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

public class RelayDisconnectListener implements DisconnectListener {
  private volatile DisconnectListener listener;
  private boolean disconnected = false;

  @Override
  public void disconnected() {
    DisconnectListener listenerToNotify = null;

    synchronized (this) {
      if (!disconnected) {
        disconnected = true;
        listenerToNotify = listener;
      }
    }

    if (listenerToNotify != null) {
      listenerToNotify.disconnected();
    }
  }

  public void setDisconnectListener(DisconnectListener incomingListener) {
    if (listener != null) {
      throw new IllegalStateException("Only one listener can be set. Attempted to set: " + incomingListener + " already set: " + listener);
    }

    boolean alreadyDisconnected;

    synchronized (this) {
      alreadyDisconnected = disconnected;

      if (!alreadyDisconnected) {
        this.listener = incomingListener;
      }
    }

    if (alreadyDisconnected) {
      incomingListener.disconnected();
    }
  }
}
