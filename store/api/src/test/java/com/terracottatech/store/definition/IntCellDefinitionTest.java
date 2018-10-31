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
import java.util.function.ToIntFunction;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    IntCellDefinition a = defineInt("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<Integer>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    IntCellDefinition a = defineInt("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Integer> value = a.valueOr(2);

    assertThat(value.apply(empty), is(2));
  }

  @Test
  public void testValueOrFailDoes() {
    IntCellDefinition a = defineInt("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Integer> value = a.valueOrFail();

    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    IntCellDefinition a = defineInt("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2));

    Function<Record<?>, Optional<Integer>> value = a.value();

    assertThat(value.apply(oneRecord), is(of(1)));
    assertThat(value.apply(twoRecord), is(of(2)));
  }

  @Test
  public void testValueOrExtracts() {
    IntCellDefinition a = defineInt("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2));

    Function<Record<?>, Integer> value = a.valueOr(3);

    assertThat(value.apply(oneRecord), is(1));
    assertThat(value.apply(twoRecord), is(2));
  }

  @Test
  public void testValueOrFailExtracts() {
    IntCellDefinition a = defineInt("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2));

    Function<Record<?>, Integer> value = a.valueOrFail();

    assertThat(value.apply(oneRecord), is(1));
    assertThat(value.apply(twoRecord), is(2));
  }

  @Test
  public void testIntegerValueOrUsesDefault() {
    IntCellDefinition a = defineInt("foo");

    Record<?> empty = mock(Record.class);

    ToIntFunction<Record<?>> value = a.intValueOr(3);

    assertThat(value.applyAsInt(empty), is(3));
  }

  @Test
  public void testIntegerValueOrFailDoes() {
    IntCellDefinition a = defineInt("foo");

    Record<?> empty = mock(Record.class);

    ToIntFunction<Record<?>> value = a.intValueOrFail();

    try {
      value.applyAsInt(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testIntegerValueOrExtracts() {
    IntCellDefinition a = defineInt("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2));

    ToIntFunction<Record<?>> value = a.intValueOr(3);

    assertThat(value.applyAsInt(oneRecord), is(1));
    assertThat(value.applyAsInt(twoRecord), is(2));
  }

  @Test
  public void testIntegerValueOrFailExtracts() {
    IntCellDefinition a = defineInt("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2));

    ToIntFunction<Record<?>> value = a.intValueOrFail();

    assertThat(value.applyAsInt(oneRecord), is(1));
    assertThat(value.applyAsInt(twoRecord), is(2));
  }
}
