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

import com.terracottatech.store.client.stream.AbstractIntStreamClosureTest;
import org.junit.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardedIntStreamClosureTest extends AbstractIntStreamClosureTest {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected IntStream getStream() {
    Stream<IntStream> shards = Stream.of(IntStream.of(1, 3), IntStream.of(2, 4));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    return new ShardedIntStream(shards, null, 0, source);
  }

  @Test
  public void testBaseStreamParallel() throws Exception {
    try {
      super.testBaseStreamParallel();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("stream has already been operated upon or closed"));
    }
  }

  @Override
  public void testBaseStreamSequential() throws Exception {
    try {
      super.testBaseStreamSequential();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("stream has already been operated upon or closed"));
    }
  }

  @Override
  public void testBaseStreamUnordered() throws Exception {
    try {
      super.testBaseStreamUnordered();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("stream has already been operated upon or closed"));
    }
  }
}
