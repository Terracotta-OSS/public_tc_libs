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
import com.terracottatech.store.common.messages.stream.RemoteStreamType;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Describes metadata of the <i>root</i> remote {@link Stream}.
 */
public interface RootStreamDescriptor {

  /**
   * Consumer to report the server execution plan to.
   *
   * @return the streams server execution plan consumer
   */
  Optional<BiConsumer<UUID, String>> getServerPlanConsumer();

  /**
   * Gets a reference to the sequence of portable, intermediate {@link PipelineOperation}s observed in the
   * pipeline.
   *
   * @return a reference to an unmodifiable view, possibly empty, of the portable, intermediate pipeline operations
   *      list; every operation will be an {@link PipelineOperation.IntermediateOperation}; the list will not be
   *      stable until the terminal operation is applied to the pipeline
   */
  List<PipelineOperation> getPortableIntermediatePipelineSequence();

  /**
   * Gets the portable terminal {@link PipelineOperation} of the pipeline.  A non-{@code null} value
   * is returned if:
   * <ul>
   *   <li>the pipeline is completely portable</li>
   *   <li>the pipeline is terminated with a non-portable, <i>consuming</i>, mutative operation</li>
   * </ul>
   * In the later case, the value returned is a proxy operation representing the non-portable mutative
   * operation.
   *
   * @return the terminal {@link PipelineOperation} (which will be a
   *         {@link PipelineOperation.TerminalOperation}) or null.
   */
  PipelineOperation getPortableTerminalOperation();

  /**
   * Gets a reference to the sequence of portable, {@link PipelineOperation}s in the client side pipeline.
   *
   * @return a reference to an unmodifiable view, possibly empty, of the clients pipeline operations list; the list will
   *      not be stable until the terminal operation is applied to the pipeline
   */
  List<PipelineOperation> getDownstreamPipelineSequence();

  /**
   * Gets the user provided desired batch size.
   *
   * @return the user-provided batch size hint
   */
  OptionalInt getBatchSizeHint();

  /**
   * Gets the requested execution mode.
   * <p>
   * An empty optional indicates that the client has no requirements regarding execution mode - the server may choose
   * and communicate the chosen mode in it's response. A non-empty return indicates the client requires execution in the
   * given mode.
   *
   * @return requested execution mode
   */
  Optional<RemoteStreamType> getExecutionMode();

  boolean isDead();

  boolean isRetryable();
}
