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
import java.util.function.Predicate;

import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BoolCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<Boolean>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Boolean> value = a.valueOr(true);

    assertThat(value.apply(empty), is(true));
  }

  @Test
  public void testValueOrFailDoes() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Boolean> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(a)).thenReturn(of(true));
    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(a)).thenReturn(of(false));

    Function<Record<?>, Optional<Boolean>> value = a.value();

    assertThat(value.apply(trueRecord), is(of(true)));
    assertThat(value.apply(falseRecord), is(of(false)));
  }

  @Test
  public void testValueOrExtracts() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(a)).thenReturn(of(true));
    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(a)).thenReturn(of(false));

    Function<Record<?>, Boolean> value = a.valueOr(true);

    assertThat(value.apply(trueRecord), is(true));
    assertThat(value.apply(falseRecord), is(false));
  }

  @Test
  public void testValueOrFailExtracts() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(a)).thenReturn(of(true));
    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(a)).thenReturn(of(false));

    Function<Record<?>, Boolean> value = a.valueOrFail();

    assertThat(value.apply(trueRecord), is(true));
    assertThat(value.apply(falseRecord), is(false));
  }

  @Test
  public void testIsTrueIsGatedProperly() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> empty = mock(Record.class);

    Predicate<Record<?>> predicate = a.isTrue();

    assertThat(predicate.test(empty), is(false));
  }

  @Test
  public void testIsFalseIsGatedProperly() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> empty = mock(Record.class);

    Predicate<Record<?>> predicate = a.isFalse();

    assertThat(predicate.test(empty), is(false));
  }

  @Test
  public void testIsTrueExtractsProperly() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(a)).thenReturn(of(true));
    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(a)).thenReturn(of(false));

    Predicate<Record<?>> predicate = a.isTrue();

    assertThat(predicate.test(trueRecord), is(true));
    assertThat(predicate.test(falseRecord), is(false));
  }

  @Test
  public void testIsFalseExtractsProperly() {
    BoolCellDefinition a = defineBool("foo");

    Record<?> trueRecord = mock(Record.class);
    when(trueRecord.get(a)).thenReturn(of(true));
    Record<?> falseRecord = mock(Record.class);
    when(falseRecord.get(a)).thenReturn(of(false));

    Predicate<Record<?>> predicate = a.isFalse();

    assertThat(predicate.test(trueRecord), is(false));
    assertThat(predicate.test(falseRecord), is(true));
  }

}
