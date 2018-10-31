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

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StringCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    StringCellDefinition a = defineString("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<String>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    StringCellDefinition a = defineString("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, String> value = a.valueOr("Batman");

    assertThat(value.apply(empty), is("Batman"));
  }

  @Test
  public void testValueOrFailDoes() {
    StringCellDefinition a = defineString("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, String> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    StringCellDefinition a = defineString("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of("foo"));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of("bar"));

    Function<Record<?>, Optional<String>> value = a.value();

    assertThat(value.apply(oneRecord), is(of("foo")));
    assertThat(value.apply(twoRecord), is(of("bar")));
  }

  @Test
  public void testValueOrExtracts() {
    StringCellDefinition a = defineString("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of("foo"));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of("bar"));

    Function<Record<?>, String> value = a.valueOr("baz");

    assertThat(value.apply(oneRecord), is("foo"));
    assertThat(value.apply(twoRecord), is("bar"));
  }

  @Test
  public void testValueOrFailExtracts() {
    StringCellDefinition a = defineString("foo");

    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of("foo"));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of("bar"));

    Function<Record<?>, String> value = a.valueOrFail();

    assertThat(value.apply(oneRecord), is("foo"));
    assertThat(value.apply(twoRecord), is("bar"));
  }
}
