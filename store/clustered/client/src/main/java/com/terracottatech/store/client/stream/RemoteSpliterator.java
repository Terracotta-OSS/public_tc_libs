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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;

import java.util.Spliterator;

/**
 * A remote {@link Spliterator} providing pipeline-related communication access to the remote server.
 */
public interface RemoteSpliterator<T> extends Spliterator<T>, RemoteObject {

  /**
   * Indicates to this {@code RemoteSpliterator} if an element <i>release</i> message should
   * be sent to the server after the element is consumed.  By default, the release message is
   * sent (<b>not</b> suppressed).  To suppress the release message, this method must be called
   * <i>before</i> the element consumer completes.
   */
  void suppressRelease();

  /**
   * Sends the {@link PipelineProcessorMessage} provided waiting for the response.
   *
   * @param action a textual indicator of the {@code pipelineProcessorMessage} operation
   * @param pipelineProcessorMessage the {@code PipelineProcessorMessage} to send
   *
   * @return the response from {@code pipelineProcessorMessage}
   */
  DatasetEntityResponse sendReceive(String action, PipelineProcessorMessage pipelineProcessorMessage);

  /**
   * A remote {@link Spliterator.OfDouble} providing pipeline-related communication access to the remote server.
   */
  interface OfDouble extends Spliterator.OfDouble, RemoteSpliterator<Double> {
  }

  /**
   * A remote {@link Spliterator.OfInt} providing pipeline-related communication access to the remote server.
   */
  interface OfInt extends Spliterator.OfInt, RemoteSpliterator<Integer> {
  }

  /**
   * A remote {@link Spliterator.OfLong} providing pipeline-related communication access to the remote server.
   */
  interface OfLong extends Spliterator.OfLong, RemoteSpliterator<Long> {
  }
}
