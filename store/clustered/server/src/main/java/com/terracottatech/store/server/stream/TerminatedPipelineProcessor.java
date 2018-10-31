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

package com.terracottatech.store.server.stream;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.Record;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.platform.ServerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static com.terracottatech.store.common.messages.DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE;
import static com.terracottatech.store.common.messages.DatasetOperationMessageType.PIPELINE_REQUEST_RESULT_MESSAGE;
import static com.terracottatech.store.common.messages.stream.ElementValue.createForObject;
import static java.util.function.Function.identity;

public class TerminatedPipelineProcessor<K extends Comparable<K>> extends PipelineProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TerminatedPipelineProcessor.class);

  private final BaseStream<?, ?> stream;
  private final Supplier<ExecuteTerminatedPipelineResponse> result;
  private volatile boolean complete;

  public TerminatedPipelineProcessor(Executor asyncExecutor, IEntityMessenger<EntityMessage, ?> entityMessenger,
                                     ServerInfo serverInfo, ClientDescriptor owner, UUID streamId,
                                     SovereignDataset<K> dataset, List<PipelineOperation> intermediateOperations,
                                     PipelineOperation terminalOperation, boolean requiresExplanation) {
    super(asyncExecutor, entityMessenger, owner, dataset.getAlias(), streamId, intermediateOperations, terminalOperation);
    this.stream = createStream(dataset, intermediateOperations, requiresExplanation);
    this.result = () ->  {
      try {
        return new ExecuteTerminatedPipelineResponse(
            getStreamId(),
            createForObject(terminatePipeline(dataset, stream, terminalOperation)),
            getExplanation().map(e -> PlanRenderer.render(serverInfo, e)).orElse(null));
      } finally {
        complete();
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <K extends Comparable<K>> Object terminatePipeline(SovereignDataset<K> dataset, BaseStream<?, ?> stream, PipelineOperation terminal) {
    List<Object> operationArguments = new ArrayList<>(terminal.getArguments());
    switch ((PipelineOperation.TerminalOperation)terminal.getOperation()) {
      case DELETE: {
        // Maps to map(dataset.delete(Duration, identity)) operation.
        return ((Stream<Record<K>>) stream).map(dataset.delete(DURABILITY, identity())).count();
      }

      case MUTATE: {
        // Maps to map(dataset.applyMutation(Duration, Function, BiFunction)) operation.
        UpdateOperation<K> transform = (UpdateOperation<K>)operationArguments.get(0);  // unchecked
        return ((Stream<Record<K>>) stream).map(dataset.applyMutation(DURABILITY, transform::apply, (oldRecord, newRecord) -> newRecord)).count();
      }

      default:
        return terminal.getOperation().reconstruct(stream, terminal.getArguments());
    }
  }

  private void complete() {
    this.complete = true;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public void close() {
    complete();
    try {
      stream.close();
    } catch (Exception e) {
      LOGGER.warn("Exception raised while closing {} - {}", getStreamIdentifier(), e, e);
    } finally {
      super.close();
    }
  }


  @Override
  public ElementType getElementType() {
    return ElementType.TERMINAL;
  }

  @Override
  public DatasetEntityResponse invoke(PipelineProcessorMessage message) {
    if (PIPELINE_REQUEST_RESULT_MESSAGE.equals(message.getType())) {
      try {
        return super.invoke(message);
      } finally {
        close();
      }
    } else {
      return super.invoke(message);
    }
  }

  public DatasetEntityResponse handleMessage(PipelineProcessorMessage msg) {
    if (EXECUTE_TERMINATED_PIPELINE_MESSAGE.equals(msg.getType())) {
      return result.get();
    } else {
      return super.handleMessage(msg);
    }
  }
}
