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

import org.junit.Rule;

import com.terracottatech.store.Record;
import com.terracottatech.test.data.Animals;

import java.util.function.ToLongFunction;
import java.util.stream.LongStream;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_LONG;

/**
 * Verifies the closure semantics of operations on a {@link RemoteLongStream}.
 */
public class RemoteLongStreamClosureTest extends AbstractLongStreamClosureTest {

  @Rule
  public EmbeddedAnimalDataset animalDataset = new EmbeddedAnimalDataset();

  @Override
  protected LongStream getStream() {
    RootRemoteRecordStream<String> rootStream = animalDataset.getStream();
    // Force into LongStream; direct access avoids intrinsic check
    rootStream.appendPortablePipelineOperation(
        MAP_TO_LONG.newInstance((ToLongFunction<Record<String>>)(r) -> r.get(Animals.Schema.OBSERVATIONS).orElse(0L)));
    return new RemoteLongStream<>(rootStream);
  }
}
