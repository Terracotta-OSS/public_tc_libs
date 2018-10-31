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

package com.terracottatech.store.client.stream;

import com.terracottatech.store.common.dataset.stream.PipelineOperation;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.join;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;

public final class SingleStripeExplanation implements Explanation {

  private final List<PipelineOperation> client;
  private final List<PipelineOperation> server;
  private final Optional<Map.Entry<?, String>> serverPlan;
  private final List<Map.Entry<UUID, String>> failedServerPlans;

  public SingleStripeExplanation(List<PipelineOperation> client, List<PipelineOperation> server, List<Map.Entry<UUID, String>> serverPlans) {
    this.client = client;
    this.server = server;
    if (serverPlans.isEmpty()) {
      this.serverPlan = Optional.empty();
      this.failedServerPlans = emptyList();
    } else {
      this.serverPlan = Optional.of(serverPlans.get(serverPlans.size() - 1));
      this.failedServerPlans = serverPlans.subList(0, serverPlans.size() - 1);
    }
  }

  @Override
  public List<PipelineOperation> clientSideMergedPipeline() {
    return emptyList();
  }

  @Override
  public List<PipelineOperation> clientSideStripePipeline() {
    return client;
  }

  @Override
  public List<PipelineOperation> serverSidePipeline() {
    return server;
  }

  @Override
  public Map<?, String> serverPlans() {
    return serverPlan.map(p -> singletonMap(p.getKey(), p.getValue())).orElse(emptyMap());
  }

  @Override
  public Map<?, String> failedServerPlans() {
    return failedServerPlans.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Stream Plan").append("\n");

    sb.append("\tStructure:\n");
    sb.append("\t\tPortable:\n").append(Explanation.formatPipeline("\t\t\t", server)).append("\n");
    sb.append("\t\tNon-Portable:\n").append(Explanation.formatPipeline("\t\t\t", client)).append("\n");

    sb.append(serverPlan.map(plan -> Explanation.formatExplanation("\t", plan)).orElse("\tNo Server Executions")).append("\n");
    failedServerPlans.stream().map(plan -> Explanation.formatExplanation("\t\t", plan))
        .reduce((a, b) -> join("\n", a, b)).ifPresent(e -> sb.append("\tFailed Server Plans:\n").append(e));
    return sb.toString();
  }
}
