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

package com.terracottatech.store.client.stream.sharded;

import com.terracottatech.store.client.stream.Explanation;
import com.terracottatech.store.client.stream.SingleStripeExplanation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Collections.emptyList;

public final class MultiStripeExplanation implements Explanation {

  private final List<SingleStripeExplanation> explanations = new CopyOnWriteArrayList<>();
  private final List<PipelineOperation> clientSideMergedPipeline = new CopyOnWriteArrayList<>();

  @Override
  public List<PipelineOperation> clientSideMergedPipeline() {
    return clientSideMergedPipeline;
  }

  @Override
  public List<PipelineOperation> clientSideStripePipeline() {
    return explanations.stream().map(Explanation::clientSideStripePipeline).findAny().orElseThrow(AssertionError::new);
  }

  @Override
  public List<PipelineOperation> serverSidePipeline() {
    return explanations.stream().map(Explanation::serverSidePipeline).findAny().orElse(emptyList());
  }

  @Override
  public Map<?, String> serverPlans() {
    return explanations.stream().flatMap(e -> e.serverPlans().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Map<?, String> failedServerPlans() {
    return explanations.stream().flatMap(e -> e.failedServerPlans().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Stream Plan").append("\n");

    sb.append("\tStructure:\n");
    sb.append("\t\tPortable:\n").append(Explanation.formatPipeline("\t\t\t", serverSidePipeline())).append("\n");
    sb.append("\t\tNon-Portable:\n");
    sb.append("\t\t\tParallel:\n").append(Explanation.formatPipeline("\t\t\t\t", clientSideStripePipeline())).append("\n");
    sb.append("\t\t\tCommon (Merged):\n").append(Explanation.formatPipeline("\t\t\t\t", clientSideMergedPipeline())).append("\n");

    sb.append(serverPlans().entrySet().stream().map(plan -> Explanation.formatExplanation("\t\t", plan))
        .reduce((a, b) -> join("\n", a, b)).map(s -> "\tServer Plans:\n" + s).orElse("\tNo Server Executions")).append("\n");
    failedServerPlans().entrySet().stream().map(plan -> Explanation.formatExplanation("\t\t", plan))
        .reduce((a, b) -> join("\n", a, b)).ifPresent(s -> sb.append("\tFailed Server Plans:\n").append(s));

    return sb.toString();
  }

  protected void addStripeExplanation(Object explanation) {
    if (explanation instanceof SingleStripeExplanation) {
      explanations.add((SingleStripeExplanation) explanation);
    } else {
      throw new AssertionError("Unexpected type received for shard explanation " + explanation);
    }
  }

  protected void addMergedPipeline(List<PipelineOperation> pipeline) {
    clientSideMergedPipeline.addAll(pipeline);
  }
}
