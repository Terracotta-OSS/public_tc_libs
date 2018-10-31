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
package com.terracottatech.store.server.management;

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.server.stream.PipelineProcessor;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COUNT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.SUM;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static java.time.Duration.ofMillis;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamTableTest {

  @Test
  public void testEmptyTable() {
    StreamTable table = new StreamTable(Duration.ofHours(1), 0.1f, comparing(StreamStatistics::getExecutionCount));

    assertThat(table.top(10), empty());
  }

  @Test
  public void testSingleRowTable() {
    float accuracy = 0.1f;
    StreamTable table = new StreamTable(Duration.ofHours(1), accuracy, comparing(StreamStatistics::getExecutionCount));

    PipelineProcessor stream = stream(COUNT.newInstance(), 10L, 5L);
    table.increment(stream);

    List<Map.Entry<StreamShape, StreamStatistics>> top = table.top(10);
    assertThat(top, hasSize(1));

    assertThat(top.get(0).getKey().toString(), is("records().count() [not-indexed]"));
    assertThat(top.get(0).getValue().getExecutionCount(), is(1L));
    assertThat(top.get(0).getValue().getTotalTime(), withinFractionalError(10, accuracy));
    assertThat(top.get(0).getValue().getServerTime(), withinFractionalError(5, accuracy));
  }

  @Test
  public void testAggregationInSingleRowTable() {
    float accuracy = 0.1f;
    StreamTable table = new StreamTable(Duration.ofHours(1), accuracy, comparing(StreamStatistics::getExecutionCount));

    PipelineProcessor stream = stream(COUNT.newInstance(), 10L, 5L);
    table.increment(stream);
    table.increment(stream);

    List<Map.Entry<StreamShape, StreamStatistics>> top = table.top(10);
    assertThat(top, hasSize(1));

    assertThat(top.get(0).getKey().toString(), is("records().count() [not-indexed]"));
    assertThat(top.get(0).getValue().getExecutionCount(), is(2L));
    assertThat(top.get(0).getValue().getTotalTime(), withinFractionalError(20, accuracy));
    assertThat(top.get(0).getValue().getServerTime(), withinFractionalError(10, accuracy));
  }

  @Test
  public void testMultipleRowTable() {
    float accuracy = 0.1f;
    StreamTable table = new StreamTable(Duration.ofHours(1), accuracy, comparing(StreamStatistics::getExecutionCount));

    table.increment(stream(COUNT.newInstance(), 10L, 5L));
    table.increment(stream(COUNT.newInstance(), 10L, 5L));
    table.increment(stream(SUM.newInstance(), 5L, 4L));

    List<Map.Entry<StreamShape, StreamStatistics>> top = table.top(10);
    assertThat(top, hasSize(2));

    assertThat(top.get(0).getKey().toString(), is("records().count() [not-indexed]"));
    assertThat(top.get(0).getValue().getExecutionCount(), is(2L));
    assertThat(top.get(0).getValue().getTotalTime(), withinFractionalError(20, accuracy));
    assertThat(top.get(0).getValue().getServerTime(), withinFractionalError(10, accuracy));

    assertThat(top.get(1).getKey().toString(), is("records().sum() [not-indexed]"));
    assertThat(top.get(1).getValue().getExecutionCount(), is(1L));
    assertThat(top.get(1).getValue().getTotalTime(), withinFractionalError(5, accuracy));
    assertThat(top.get(1).getValue().getServerTime(), withinFractionalError(4, accuracy));
  }

  @Test
  public void testSubsetSelection() {
    float accuracy = 0.1f;
    StreamTable table = new StreamTable(Duration.ofHours(1), accuracy, comparing(StreamStatistics::getExecutionCount));

    table.increment(stream(COUNT.newInstance(), 10L, 5L));
    table.increment(stream(COUNT.newInstance(), 10L, 5L));
    table.increment(stream(SUM.newInstance(), 5L, 4L));

    List<Map.Entry<StreamShape, StreamStatistics>> top = table.top(1);
    assertThat(top, hasSize(1));

    assertThat(top.get(0).getKey().toString(), is("records().count() [not-indexed]"));
    assertThat(top.get(0).getValue().getExecutionCount(), is(2L));
    assertThat(top.get(0).getValue().getTotalTime(), withinFractionalError(20, accuracy));
    assertThat(top.get(0).getValue().getServerTime(), withinFractionalError(10, accuracy));
  }

  @Test
  public void testExpirationByQuery() throws TimeoutException {
    StreamTable table = new StreamTable(ofMillis(100), 0.1f, comparing(StreamStatistics::getExecutionCount));

    table.increment(stream(COUNT.newInstance(), 10L, 5L));

    assertThatEventually(() -> table.top(1), hasSize(0)).within(ofMillis(2000));
  }

  @Test
  public void testHighAccuracy() {
    float accuracy = 0.01f;
    StreamTable table = new StreamTable(Duration.ofHours(1), accuracy, comparing(StreamStatistics::getExecutionCount));

    long seed = System.nanoTime();
    Random rndm = new Random(seed);
    try {
      long countTotal1 = rndm.nextInt(Integer.MAX_VALUE);
      long countServer1 = rndm.nextInt((int) countTotal1);
      long countTotal2 = rndm.nextInt(Integer.MAX_VALUE);
      long countServer2 = rndm.nextInt((int) countTotal2);
      table.increment(stream(COUNT.newInstance(), countTotal1, countServer1));
      table.increment(stream(COUNT.newInstance(), countTotal2, countServer2));

      long sumTotal = rndm.nextInt(Integer.MAX_VALUE);
      long sumServer = rndm.nextInt((int) sumTotal);
      table.increment(stream(SUM.newInstance(), sumTotal, sumServer));

      List<Map.Entry<StreamShape, StreamStatistics>> top = table.top(10);
      assertThat(top, hasSize(2));

      long countTotal = countTotal1 + countTotal2;
      long countServer = countServer1 + countServer2;
      assertThat(top.get(0).getKey().toString(), is("records().count() [not-indexed]"));
      assertThat(top.get(0).getValue().getExecutionCount(), is(2L));
      assertThat(top.get(0).getValue().getTotalTime(), withinFractionalError(countTotal, accuracy));
      assertThat(top.get(0).getValue().getServerTime(), withinFractionalError(countServer, accuracy));

      assertThat(top.get(1).getKey().toString(), is("records().sum() [not-indexed]"));
      assertThat(top.get(1).getValue().getExecutionCount(), is(1L));
      assertThat(top.get(1).getValue().getTotalTime(), withinFractionalError(sumTotal, accuracy));
      assertThat(top.get(1).getValue().getServerTime(), withinFractionalError(sumServer, accuracy));
    } catch (Throwable t) {
      throw new AssertionError("Failure with seed: " + seed, t);
    }
  }

  private PipelineProcessor stream(PipelineOperation operation, long totalTime, long serverTime) {
    PipelineProcessor stream = mock(PipelineProcessor.class);

    StreamShape shape = StreamShape.shapeOf(emptyList(), operation);
    shape.setIndexed(false);
    when(stream.getStreamShape()).thenReturn(shape);
    when(stream.getServerTime()).thenReturn(serverTime);
    when(stream.getTotalTime()).thenReturn(totalTime);

    return stream;
  }

  private Matcher<Long> withinFractionalError(long value, double fraction) {
    Matcher<Double> matcher = closeTo((double) value, Math.ceil(value * fraction));
    return new TypeSafeMatcher<Long>() {
      @Override
      protected boolean matchesSafely(Long item) {
        return matcher.matches(item.doubleValue());
      }

      @Override
      public void describeTo(Description description) {
        matcher.describeTo(description);
      }
    };
  }
}
