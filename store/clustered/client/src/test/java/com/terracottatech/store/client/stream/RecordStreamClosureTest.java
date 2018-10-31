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

import java.util.stream.Stream;

import static org.junit.Assume.assumeTrue;

/**
 * Verifies the closure semantics of operations on a {@link RemoteRecordStream}.
 */
public class RecordStreamClosureTest extends AbstractStreamClosureTest<Record<String>> {

  @Rule
  public EmbeddedAnimalDataset animalDataset = new EmbeddedAnimalDataset();

  @Override
  protected Stream<Record<String>> getStream() {
    return animalDataset.getStream();
  }

  @Override
  public void testSorted0Arg() throws Exception {
    assumeTrue("Test skipped; Record is not Comparable", Comparable.class.isAssignableFrom(Record.class));
    super.testSorted0Arg();
  }
}
