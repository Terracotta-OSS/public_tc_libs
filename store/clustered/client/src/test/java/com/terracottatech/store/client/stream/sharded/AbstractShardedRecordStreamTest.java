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
import com.terracottatech.store.client.stream.Explanation;
import com.terracottatech.store.client.stream.SingleStripeExplanation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;
import com.terracottatech.store.stream.RecordStream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.SUM;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class AbstractShardedRecordStreamTest {

  @Test
  public void testDefaultCharacteristics() {
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(emptyList());
    assertThat(stream.with(), is(CONCURRENT | NONNULL | DISTINCT));
  }

  @Test
  public void testDefaultMergedPipelineIsEmpty() {
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(emptyList());
    assertThat(stream.getMergedPipeline(), empty());
  }

  @Test
  public void testRegisteredMergedPipelineIsReturned() {
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(emptyList());

    List<PipelineOperation> pipeline = mock(List.class);
    WrappedReferenceStream<String> commonStream = mock(WrappedReferenceStream.class);
    when(commonStream.getPipeline()).thenReturn(pipeline);

    assertThat(stream.registerCommonStream(commonStream), sameInstance(commonStream));
    assertThat(stream.getMergedPipeline(), sameInstance(pipeline));
  }

  @Test
  public void testExplainDelegatesToShards() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.explain(any(Consumer.class))).then(RETURNS_SELF);
    when(b.explain(any(Consumer.class))).then(RETURNS_SELF);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    stream.explain(mock(Consumer.class)).count();

    verify(a).explain(any(Consumer.class));
    verify(b).explain(any(Consumer.class));
  }

  @Test
  public void testRequiresExplanationAfterExplain() {
    AbstractShardedRecordStream source = new Impl(emptyList());
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(Stream.empty(), null, 0, source);

    stream.explain(mock(Consumer.class));

    assertTrue(stream.requiresExplanation());
    assertTrue(source.requiresExplanation());
  }

  @Test
  public void testExplainCallsConsumer() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.explain(any(Consumer.class))).then(RETURNS_SELF);
    when(b.explain(any(Consumer.class))).then(RETURNS_SELF);
    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    Consumer<Object> consumer = mock(Consumer.class);

    stream.explain(consumer).count();

    verify(consumer).accept(any());
  }

  @Test
  public void testExplainCapturesMergedPipeline() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.explain(any(Consumer.class))).then(RETURNS_SELF);
    when(b.explain(any(Consumer.class))).then(RETURNS_SELF);
    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream<String, RecordStream<String>> source = new Impl(emptyList());
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    Object[] arr = new Object[1];
    RecordStream<String> explained = stream.explain(e -> arr[0] = e);
    explained.count();

    Object explanation = arr[0];
    assertThat(explanation, instanceOf(Explanation.class));
    assertThat(((Explanation) explanation).clientSideMergedPipeline()
        .stream().map(PipelineOperation::getOperation).collect(toList()), contains(SUM));
  }

  @Test
  public void testExplainCapturesShardExplanation() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.explain(any(Consumer.class))).then(RETURNS_SELF);
    when(b.explain(any(Consumer.class))).then(RETURNS_SELF);
    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream<String, RecordStream<String>> source = new Impl(emptyList());
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    Object[] arr = new Object[1];
    RecordStream<String> explained = stream.explain(e -> arr[0] = e);

    explained.count();

    ArgumentCaptor<Consumer<Object>> shardExplainConsumer = ArgumentCaptor.forClass(Consumer.class);
    verify(a).explain(shardExplainConsumer.capture());

    shardExplainConsumer.getValue().accept(new SingleStripeExplanation(emptyList(), emptyList(), singletonList(new AbstractMap.SimpleImmutableEntry<>(UUID.randomUUID(), "DIS AR TEH WUT I DID"))));

    Object explanation = arr[0];
    assertThat(explanation, instanceOf(Explanation.class));
    assertThat(((Explanation) explanation).serverPlans(), hasValue("DIS AR TEH WUT I DID"));
  }

  @Test
  public void testBatchIsSharded() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.batch(anyInt())).then(RETURNS_SELF);
    when(b.batch(anyInt())).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.batch(42);

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).batch(42);
    verify(b).batch(42);
  }

  @Test
  public void testInlineIsSharded() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.inline()).then(RETURNS_SELF);
    when(b.inline()).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.inline();

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).inline();
    verify(b).inline();
  }

  @Test
  public void testFilterIsDoneInParallel() {
    Record<String> a = mock(Record.class);
    when(a.getKey()).thenReturn("a");
    Record<String> b = mock(Record.class);
    when(b.getKey()).thenReturn("b");
    Record<String> c = mock(Record.class);
    when(c.getKey()).thenReturn("c");
    Record<String> d = mock(Record.class);
    when(d.getKey()).thenReturn("d");
    Stream<Stream<Record<String>>> shards = Stream.of(Stream.of(a, b), Stream.of(c, d));

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.filter(r -> r.getKey().compareTo("b") >= 0);

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.map(Record::getKey).collect(toList()), containsInAnyOrder("b", "c", "d"));
  }

  @Test
  public void testDistinctReturnsThis() {
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(emptyList());
    assertThat(stream.distinct(), sameInstance(stream));
  }

  @Test
  public void testNaturalSortFailsEagerly() {
    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(emptyList());
    try {
      stream.sorted();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      assertThat(e.getMessage(), containsString("sorted(keyFunction().asComparator())"));
    }
  }

  @Test
  public void testSortedShardsAndDetaches() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.sorted(any(Comparator.class))).then(RETURNS_SELF);
    when(b.sorted(any(Comparator.class))).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    Comparator<Record<String>> comparator = Record.<String>keyFunction().asComparator();

    RecordStream<String> derived = stream.sorted(comparator);

    assertThat(derived, instanceOf(DetachedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).sorted(comparator);
    verify(b).sorted(comparator);
  }

  @Test
  public void testPeekIsDoneInParallel() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.peek(any(Consumer.class))).then(RETURNS_SELF);
    when(b.peek(any(Consumer.class))).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    Consumer<Record<String>> consumer = record -> {};
    RecordStream<String> derived = stream.peek(consumer);

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).peek(consumer);
    verify(b).peek(consumer);
  }

  @Test
  public void testSequentialDelegatesToShards() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.sequential()).then(RETURNS_SELF);
    when(b.sequential()).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.sequential();

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).sequential();
    verify(b).sequential();
  }

  @Test
  public void testParallelDelegatesToShards() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.parallel()).then(RETURNS_SELF);
    when(b.parallel()).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.parallel();

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).parallel();
    verify(b).parallel();
  }

  @Test
  public void testUnorderedDelegatesToShards() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.unordered()).then(RETURNS_SELF);
    when(b.unordered()).then(RETURNS_SELF);
    when(a.count()).thenReturn(0L);
    when(b.count()).thenReturn(0L);

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    AbstractShardedRecordStream<String, RecordStream<String>> stream = new Impl(shards, null, 0, source);

    RecordStream<String> derived = stream.unordered();

    assertThat(derived, instanceOf(AbstractShardedRecordStream.class));
    assertThat(derived.count(), is(0L));

    verify(a).unordered();
    verify(b).unordered();
  }

  static class Impl extends AbstractShardedRecordStream<String, RecordStream<String>> {

    protected Impl(List<RecordStream<String>> initial) {
      super(initial);
    }

    protected Impl(Stream<Stream<Record<String>>> shards, Comparator<? super Record<String>> ordering, int characteristics, AbstractShardedRecordStream<String, ?> source) {
      super(shards, ordering, characteristics, source);
    }

    @Override
    protected RecordStream<String> wrapAsRecordStream(Stream<Stream<Record<String>>> shards, Comparator<? super Record<String>> ordering, int characteristics) {
      return new Impl(shards, ordering, characteristics, getSource());
    }

    @Override
    public RecordStream<String> limit(long maxSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordStream<String> skip(long n) {
      throw new UnsupportedOperationException();
    }
  }
}
