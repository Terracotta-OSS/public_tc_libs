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
import java.util.stream.Collectors;

public interface Explanation {

  List<PipelineOperation> clientSideMergedPipeline();

  List<PipelineOperation> clientSideStripePipeline();

  List<PipelineOperation> serverSidePipeline();

  Map<?, String> serverPlans();

  Map<?, String> failedServerPlans();

  static CharSequence formatPipeline(String prefix, List<PipelineOperation> pipeline) {
    if (pipeline.isEmpty()) {
      return prefix + "None";
    } else {
      return pipeline.stream().map(p -> prefix + p.toString()).collect(Collectors.joining("\n"));
    }
  }

  static CharSequence formatExplanation(String prefix, Map.Entry<?, String> explanation) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix).append("Server Plan: [stream id: ").append(explanation.getKey()).append("]\n");
    //"(?m)" turns on multiline matching, "^" then matches beginning of every line
    sb.append(explanation.getValue().toString().replaceAll("(?m)^", prefix + "\t"));
    return sb;
  }
}
