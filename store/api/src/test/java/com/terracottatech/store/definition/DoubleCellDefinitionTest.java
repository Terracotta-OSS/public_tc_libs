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

package com.terracottatech.store.definition;

import com.terracottatech.store.Record;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DoubleCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<Double>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Double> value = a.valueOr(Math.E);

    assertThat(value.apply(empty), is(Math.E));
  }

  @Test
  public void testValueOrFailDoes() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Double> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> piRecord = mock(Record.class);
    when(piRecord.get(a)).thenReturn(of(Math.PI));
    Record<?> eRecord = mock(Record.class);
    when(eRecord.get(a)).thenReturn(of(Math.E));

    Function<Record<?>, Optional<Double>> value = a.value();

    assertThat(value.apply(piRecord), is(of(Math.PI)));
    assertThat(value.apply(eRecord), is(of(Math.E)));
  }

  @Test
  public void testValueOrExtracts() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> piRecord = mock(Record.class);
    when(piRecord.get(a)).thenReturn(of(Math.PI));
    Record<?> eRecord = mock(Record.class);
    when(eRecord.get(a)).thenReturn(of(Math.E));

    Function<Record<?>, Double> value = a.valueOr(0.0);

    assertThat(value.apply(piRecord), is(Math.PI));
    assertThat(value.apply(eRecord), is(Math.E));
  }

  @Test
  public void testValueOrFailExtracts() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> piRecord = mock(Record.class);
    when(piRecord.get(a)).thenReturn(of(Math.PI));
    Record<?> eRecord = mock(Record.class);
    when(eRecord.get(a)).thenReturn(of(Math.E));

    Function<Record<?>, Double> value = a.valueOrFail();

    assertThat(value.apply(piRecord), is(Math.PI));
    assertThat(value.apply(eRecord), is(Math.E));
  }

  @Test
  public void testDoubleValueOrUsesDefault() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> empty = mock(Record.class);

    ToDoubleFunction<Record<?>> value = a.doubleValueOr(Math.E);

    assertThat(value.applyAsDouble(empty), is(Math.E));
  }

  @Test
  public void testDoubleValueOrFailDoes() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> empty = mock(Record.class);

    ToDoubleFunction<Record<?>> value = a.doubleValueOrFail();


    try {
      value.applyAsDouble(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testDoubleValueOrExtracts() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> piRecord = mock(Record.class);
    when(piRecord.get(a)).thenReturn(of(Math.PI));
    Record<?> eRecord = mock(Record.class);
    when(eRecord.get(a)).thenReturn(of(Math.E));

    ToDoubleFunction<Record<?>> value = a.doubleValueOr(0.0);

    assertThat(value.applyAsDouble(piRecord), is(Math.PI));
    assertThat(value.applyAsDouble(eRecord), is(Math.E));
  }

  @Test
  public void testDoubleValueOrFailExtracts() {
    DoubleCellDefinition a = defineDouble("foo");

    Record<?> piRecord = mock(Record.class);
    when(piRecord.get(a)).thenReturn(of(Math.PI));
    Record<?> eRecord = mock(Record.class);
    when(eRecord.get(a)).thenReturn(of(Math.E));

    ToDoubleFunction<Record<?>> value = a.doubleValueOrFail();

    assertThat(value.applyAsDouble(piRecord), is(Math.PI));
    assertThat(value.applyAsDouble(eRecord), is(Math.E));
  }
}
