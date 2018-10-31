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

import com.terracottatech.store.Record;
import com.terracottatech.store.client.stream.AbstractStreamClosureTest;
import org.junit.AssumptionViolatedException;

import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class DetachedRecordStreamClosureTest extends AbstractStreamClosureTest<Record<String>> {

  @Override
  protected Stream<Record<String>> getStream() {
    @SuppressWarnings("unchecked")
    Record<String> record = mock(Record.class);
    return new DetachedRecordStream<>(emptyList(), Stream.of(record, record));
  }

  @Override
  public void testBaseStreamOnClose() throws Exception {
    BaseStream<?, ?> stream = close(getStream());
    if (javaMajor() >= 9) {
      assertThrows(() -> stream.onClose(() -> { }), IllegalStateException.class);
    } else {
      assertNotNull(stream.onClose(() -> {}));
    }
  }

  @Override
  public void testDistinct() throws Exception {
    throw new AssumptionViolatedException("ShardedRecordStream elides distinct operations");
  }
}
