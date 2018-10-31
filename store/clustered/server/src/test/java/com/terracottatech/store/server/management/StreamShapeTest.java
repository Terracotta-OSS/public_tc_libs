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

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import org.junit.Test;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DISTINCT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.MAP_TO_INT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.SORTED_0;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StreamShapeTest {

  @Test
  public void testEmptyPipeline() {
    StreamShape shape = StreamShape.shapeOf(emptyList(), null);
    assertThat(shape.toString(), is("records() [indexing-unknown]"));
  }

  @Test
  public void testTerminalOnlyPipeline() {
    StreamShape shape = StreamShape.shapeOf(emptyList(), COUNT.newInstance());
    assertThat(shape.toString(), is("records().count() [indexing-unknown]"));
  }

  @Test
  public void testIntermediateOnlyPipeline() {
    StreamShape shape = StreamShape.shapeOf(singletonList(DISTINCT.newInstance()), null);
    assertThat(shape.toString(), is("records().distinct() [indexing-unknown]"));
  }

  @Test
  public void testTwoIntermediates() {
    StreamShape shape = StreamShape.shapeOf(asList(DISTINCT.newInstance(), SORTED_0.newInstance()), null);
    assertThat(shape.toString(), is("records().distinct().sorted() [indexing-unknown]"));
  }

  @Test
  public void testConstantParameter() {
    IntCellDefinition foo = CellDefinition.defineInt("foo");
    StreamShape shape = StreamShape.shapeOf(singletonList(FILTER.newInstance(foo.value().isGreaterThan(42))), null);
    assertThat(shape.toString(), is("records().filter((foo>?)) [indexing-unknown]"));
  }

  @Test
  public void testCamelCasing() {
    IntCellDefinition foo = CellDefinition.defineInt("foo");
    StreamShape shape = StreamShape.shapeOf(singletonList(MAP_TO_INT.newInstance(foo.intValueOr(42))), null);
    assertThat(shape.toString(), is("records().mapToInt(foo.intValueOr(42)) [indexing-unknown]"));
  }

  @Test
  public void testIndexingOn() {
    StreamShape shape = StreamShape.shapeOf(singletonList(SORTED_0.newInstance()), null);
    shape.setIndexed(true);
    assertThat(shape.toString(), is("records().sorted() [indexed]"));
  }

  @Test
  public void testIndexingOff() {
    StreamShape shape = StreamShape.shapeOf(singletonList(SORTED_0.newInstance()), null);
    shape.setIndexed(false);
    assertThat(shape.toString(), is("records().sorted() [not-indexed]"));
  }
}