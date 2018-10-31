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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ChangeRecorder implements ChangeListener<Integer> {
  private final ArrayList<ChangeEvent> listenerChangeList;
  private volatile boolean missedEvent = false;
  private volatile CountDownLatch countDownLatch;

  public ChangeRecorder(int expectedEventCount) {
    listenerChangeList = new ArrayList<>();
    countDownLatch = new CountDownLatch(expectedEventCount);
  }

  @Override
  public void onChange(Integer key, ChangeType changeType) {
    listenerChangeList.add(new ChangeEvent(key, changeType));
    countDownLatch.countDown();
  }

  @Override
  public void missedEvents() {
    missedEvent = true;
  }

  public void resetExpectedEventCount(int expectedEventCount) {
    countDownLatch = new CountDownLatch(expectedEventCount);
  }

  public void waitForEventCountDown() throws Exception {
    countDownLatch.await(10_000, TimeUnit.MILLISECONDS);
  }

  public boolean hasProcessedMissedEvent() throws Exception {
    return missedEvent;
  }

  public boolean changeListContainsInAnyOrder(List<ChangeEvent> changeEventList) {
    return listenerChangeList.containsAll(changeEventList);
  }
}
