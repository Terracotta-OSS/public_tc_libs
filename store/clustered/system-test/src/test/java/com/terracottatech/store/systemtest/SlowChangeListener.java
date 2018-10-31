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

package com.terracottatech.store.systemtest;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SlowChangeListener implements ChangeListener<Integer> {
  private final CountDownLatch listenerBlocker;
  private final CountDownLatch missingEventIdentifier;

  public SlowChangeListener() {
    this.listenerBlocker = new CountDownLatch(1);
    this.missingEventIdentifier = new CountDownLatch(1);
  }

  @Override
  public synchronized void onChange(Integer key, ChangeType changeType) {
    try {
      listenerBlocker.await(10_000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void missedEvents() {
    missingEventIdentifier.countDown();
  }

  public void unblockListener() {
    listenerBlocker.countDown();
  }

  public boolean hasProcessedMissedEvent() throws Exception {
    return missingEventIdentifier.await(10_000, TimeUnit.MILLISECONDS);
  }
}
