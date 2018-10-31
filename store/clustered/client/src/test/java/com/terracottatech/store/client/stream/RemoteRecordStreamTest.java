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

import org.junit.Test;

import com.terracottatech.store.Record;
import com.terracottatech.test.data.Animals;

import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Argument validity tests against {@link RemoteRecordStream}.  See {@link RemoteRecordStreamEmbeddedTest}
 * for functional tests.
 */
public class RemoteRecordStreamTest extends AbstractRemoteStreamTest<String, Record<String>, Stream<Record<String>>, RemoteRecordStream<String>> {

  @SuppressWarnings("unchecked")
  public RemoteRecordStreamTest() {
    super(null, (Class)RemoteRecordStream.class, (Class)Stream.class);    // unchecked
  }


  @Test
  public void testFilter() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.filter(null), NullPointerException.class);
    });
  }

  @Test
  public void testMap() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.map(null), NullPointerException.class);
    });
  }

  @Test
  public void testMapToDouble() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.mapToDouble(null), NullPointerException.class);
    });
  }

  @Test
  public void testMapToInt() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.mapToInt(null), NullPointerException.class);
    });
  }

  @Test
  public void testMapToLong() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.mapToLong(null), NullPointerException.class);
    });
  }

  @Test
  public void testFlatMap() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.flatMap(null), NullPointerException.class);
    });
  }

  @Test
  public void testFlatMapToDouble() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToDouble(null), NullPointerException.class);
    });
  }

  @Test
  public void testFlatMapToInt() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToInt(null), NullPointerException.class);
    });
  }

  @Test
  public void testFlatMapToLong() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.flatMapToLong(null), NullPointerException.class);
    });
  }

  @Test
  public void testSorted1Arg() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.sorted(null), NullPointerException.class);
    });
  }

  @Test
  public void testPeek() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.peek(null), NullPointerException.class);
    });
  }

  @Test
  public void testLimit() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.limit(-1), IllegalArgumentException.class);
    });
  }

  @Test
  public void testSkip() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.skip(-1), IllegalArgumentException.class);
    });
  }

  @Test
  public void testForEach() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.forEach(null), NullPointerException.class);
    });
  }

  @Test
  public void testForEachOrdered() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.forEachOrdered(null), NullPointerException.class);
    });
  }

  @Test
  public void testToArray1Arg() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.toArray(null), NullPointerException.class);
    });
  }

  @Test
  public void testReduce1Arg() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.reduce(null), NullPointerException.class);
    });
  }

  @Test
  public void testReduce2Arg() throws Exception {
    Record<String> identity = null;
    tryStream(stream -> {
      // identity can be null
      assertThrows(() -> stream.reduce(identity, null), NullPointerException.class);
    });
  }

  @Test
  public void testReduce3Arg() throws Exception {
    Record<String> identity = null;
    BinaryOperator<Record<String>> accumulator = (r1, r2) -> r2;
    BinaryOperator<Record<String>> combiner  = (r1, r2) -> r2;
    tryStream(stream -> {
      // identity can be null
      assertThrows(() -> stream.reduce(identity, null, combiner), NullPointerException.class);
      assertThrows(() -> stream.reduce(identity, accumulator, null), NullPointerException.class);
    });
  }

  @Test
  public void testCollect1Arg() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.collect(null), NullPointerException.class);
    });
  }

  @Test
  public void testCollect3Arg() throws Exception {
    Supplier<ArrayList<Object>> supplier = ArrayList::new;
    BiConsumer<ArrayList<Object>, Record<String>> accumulator = ArrayList::add;
    BiConsumer<ArrayList<Object>, ArrayList<Object>> combiner = ArrayList::addAll;
    tryStream(stream -> {
      assertThrows(() -> stream.collect(null, accumulator, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, null, combiner), NullPointerException.class);
      assertThrows(() -> stream.collect(supplier, accumulator, null), NullPointerException.class);
    });
  }

  @Test
  public void testMin() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.min(null), NullPointerException.class);
    });
  }

  @Test
  public void testMax() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.max(null), NullPointerException.class);
    });
  }

  @Test
  public void testAnyMatch() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.anyMatch(null), NullPointerException.class);
    });
  }

  @Test
  public void testAllMatch() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.allMatch(null), NullPointerException.class);
    });
  }

  @Test
  public void testNoneMatch() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.noneMatch(null), NullPointerException.class);
    });
  }

  @Test
  public void testExplain() throws Exception {
    tryStream(stream -> {
      assertThrows(() -> stream.explain(null), NullPointerException.class);
    });
  }


  @Override
  protected RemoteRecordStream<String> getTestStream() {
    return animalDataset.getStream();
  }

  @Override
  protected Stream<Record<String>> getExpectedStream() {
    return Animals.recordStream();
  }
}
