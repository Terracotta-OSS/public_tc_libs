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
package com.terracottatech.store.management;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

public class CollectingManageableObserver implements ManageableObserver {

  private final Queue<ContextualNotification> notificationQueue;
  private final Queue<Collection<ContextualStatistics>> statisticQueue;

  public CollectingManageableObserver() {
    this(new LinkedBlockingQueue<>(), new LinkedBlockingQueue<>());
  }

  public CollectingManageableObserver(BlockingQueue<ContextualNotification> notificationQueue, BlockingQueue<Collection<ContextualStatistics>> statisticQueue) {
    this.notificationQueue = notificationQueue;
    this.statisticQueue = statisticQueue;
  }

  public Stream<ContextualNotification> streamNotifications() {
    return notificationQueue == null ? Stream.empty() : notificationQueue.stream();
  }

  public Stream<Collection<ContextualStatistics>> streamStatistics() {
    return statisticQueue == null ? Stream.empty() : statisticQueue.stream();
  }

  @Override
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  public void onNotification(ContextualNotification notification) {
    if (this.notificationQueue != null) {
      this.notificationQueue.offer(notification);
    }
  }

  @Override
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  public void onStatistics(Collection<ContextualStatistics> statistics) {
    if (this.statisticQueue != null) {
      this.statisticQueue.offer(statistics);
    }
  }

}
