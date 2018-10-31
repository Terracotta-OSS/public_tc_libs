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

import static com.terracottatech.store.definition.CellDefinition.defineChar;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CharCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    CharCellDefinition a = defineChar("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<Character>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    CharCellDefinition a = defineChar("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Character> value = a.valueOr('☃');

    assertThat(value.apply(empty), is('☃'));
  }

  @Test
  public void testValueOrFailDoes() {
    CharCellDefinition a = defineChar("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Character> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    CharCellDefinition a = defineChar("foo");

    Record<?> snowRecord = mock(Record.class);
    when(snowRecord.get(a)).thenReturn(of('☃'));
    Record<?> skullRecord = mock(Record.class);
    when(skullRecord.get(a)).thenReturn(of('☠'));

    Function<Record<?>, Optional<Character>> value = a.value();

    assertThat(value.apply(snowRecord), is(of('☃')));
    assertThat(value.apply(skullRecord), is(of('☠')));
  }

  @Test
  public void testValueOrExtracts() {
    CharCellDefinition a = defineChar("foo");

    Record<?> snowRecord = mock(Record.class);
    when(snowRecord.get(a)).thenReturn(of('☃'));
    Record<?> skullRecord = mock(Record.class);
    when(skullRecord.get(a)).thenReturn(of('☠'));

    Function<Record<?>, Character> value = a.valueOr('☘');

    assertThat(value.apply(snowRecord), is('☃'));
    assertThat(value.apply(skullRecord), is('☠'));
  }

  @Test
  public void testValueOrFailExtracts() {
    CharCellDefinition a = defineChar("foo");

    Record<?> snowRecord = mock(Record.class);
    when(snowRecord.get(a)).thenReturn(of('☃'));
    Record<?> skullRecord = mock(Record.class);
    when(skullRecord.get(a)).thenReturn(of('☠'));

    Function<Record<?>, Character> value = a.valueOrFail();

    assertThat(value.apply(snowRecord), is('☃'));
    assertThat(value.apply(skullRecord), is('☠'));
  }
}
