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
import java.util.function.ToLongFunction;

import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LongCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    LongCellDefinition a = defineLong("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<Long>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    LongCellDefinition a = defineLong("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Long> value = a.valueOr(2L);

    assertThat(value.apply(empty), is(2L));
  }

  @Test
  public void testValueOrFailDoes() {
    LongCellDefinition a = defineLong("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Long> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    LongCellDefinition a = defineLong("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1L));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2L));

    Function<Record<?>, Optional<Long>> value = a.value();

    assertThat(value.apply(oneRecord), is(of(1L)));
    assertThat(value.apply(twoRecord), is(of(2L)));
  }

  @Test
  public void testValueOrExtracts() {
    LongCellDefinition a = defineLong("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1L));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2L));

    Function<Record<?>, Long> value = a.valueOr(3L);

    assertThat(value.apply(oneRecord), is(1L));
    assertThat(value.apply(twoRecord), is(2L));
  }

  @Test
  public void testValueOrFailExtracts() {
    LongCellDefinition a = defineLong("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1L));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2L));

    Function<Record<?>, Long> value = a.valueOrFail();

    assertThat(value.apply(oneRecord), is(1L));
    assertThat(value.apply(twoRecord), is(2L));
  }

  @Test
  public void testLongValueOrUsesDefault() {
    LongCellDefinition a = defineLong("foo");

    Record<?> empty = mock(Record.class);

    ToLongFunction<Record<?>> value = a.longValueOr(3L);

    assertThat(value.applyAsLong(empty), is(3L));
  }

  @Test
  public void testLongValueOrFailDoes() {
    LongCellDefinition a = defineLong("foo");

    Record<?> empty = mock(Record.class);

    ToLongFunction<Record<?>> value = a.longValueOrFail();


    try {
      value.applyAsLong(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testLongValueOrExtracts() {
    LongCellDefinition a = defineLong("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1L));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2L));

    ToLongFunction<Record<?>> value = a.longValueOr(3L);

    assertThat(value.applyAsLong(oneRecord), is(1L));
    assertThat(value.applyAsLong(twoRecord), is(2L));
  }

  @Test
  public void testLongValueOrFailExtracts() {
    LongCellDefinition a = defineLong("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(1L));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(2L));

    ToLongFunction<Record<?>> value = a.longValueOrFail();

    assertThat(value.applyAsLong(oneRecord), is(1L));
    assertThat(value.applyAsLong(twoRecord), is(2L));
  }
}
