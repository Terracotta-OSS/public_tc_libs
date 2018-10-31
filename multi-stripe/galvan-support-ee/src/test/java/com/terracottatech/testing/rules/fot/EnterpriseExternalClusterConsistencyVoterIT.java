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
package com.terracottatech.testing.rules.fot;

import org.junit.Test;
import org.terracotta.voter.VoterStatus;

import com.terracottatech.voter.EnterpriseTCVoterImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EnterpriseExternalClusterConsistencyVoterIT extends EnterpriseExternalClusterITBase {

  @Override
  protected int failoverPriorityVoterCount() {
    return 1;
  }

  @Test
  public void testFailover() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    EnterpriseTCVoterImpl voter = new EnterpriseTCVoterImpl();
    Future<VoterStatus> voterStatusFuture = voter.register("MyCluster", CLUSTER.getClusterHostPorts());
    VoterStatus voterStatus = voterStatusFuture.get();
    voterStatus.awaitRegistrationWithAll(10, TimeUnit.SECONDS);

    //Fail-over should since the cluster is tuned for consistency and there is a voter to vote and promote tha passive

    CompletableFuture<Void> connectionFuture = CompletableFuture.runAsync(() -> {
      try {
        CLUSTER.getClusterControl().waitForActive();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    connectionFuture.get(10, TimeUnit.SECONDS);
  }

}
