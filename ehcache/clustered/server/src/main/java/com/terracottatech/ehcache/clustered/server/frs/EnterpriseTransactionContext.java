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
package com.terracottatech.ehcache.clustered.server.frs;

import org.ehcache.clustered.server.state.EhcacheStateContext;

import com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSService;

public class EnterpriseTransactionContext implements EhcacheStateContext {
  private final EhcacheFRSService ehcacheFRSService;
  private final String tierManagerId;
  private final String tierId;

  public EnterpriseTransactionContext(EhcacheFRSService ehcacheFRSService, String tierManagerId, String tierId) {
    this.ehcacheFRSService = ehcacheFRSService;
    this.tierManagerId = tierManagerId;
    this.tierId = tierId;
    this.ehcacheFRSService.beginTransaction(tierManagerId, tierId);
  }

  @Override
  public void close() {
    this.ehcacheFRSService.endTransaction(tierManagerId, tierId);
  }
}
