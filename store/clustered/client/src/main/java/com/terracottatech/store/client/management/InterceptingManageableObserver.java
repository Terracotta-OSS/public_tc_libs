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
package com.terracottatech.store.client.management;

import com.terracottatech.store.management.ManageableObserver;
import org.terracotta.management.entity.nms.agent.client.NmsAgentService;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.Collection;
import java.util.Objects;

public class InterceptingManageableObserver implements ManageableObserver {

  private final ManageableObserver delegate;
  private final NmsAgentService nmsAgentService;

  InterceptingManageableObserver(ManageableObserver delegate, NmsAgentService nmsAgentService) {
    this.delegate = Objects.requireNonNull(delegate);
    this.nmsAgentService = Objects.requireNonNull(nmsAgentService);
  }

  @Override
  public void onNotification(ContextualNotification notification) {
    if (!nmsAgentService.isDisconnected()) {
      nmsAgentService.pushNotification(notification);
    }
    delegate.onNotification(notification);
  }

  @Override
  public void onStatistics(Collection<ContextualStatistics> statistics) {
    if (!nmsAgentService.isDisconnected()) {
      nmsAgentService.pushStatistics(statistics);
    }
    delegate.onStatistics(statistics);
  }

}
