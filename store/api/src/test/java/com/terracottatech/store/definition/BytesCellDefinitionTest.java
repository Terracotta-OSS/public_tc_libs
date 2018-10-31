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

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import static com.terracottatech.store.definition.CellDefinition.defineBytes;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BytesCellDefinitionTest {

  @Test
  public void testValueExtractsEmpty() {
    BytesCellDefinition a = defineBytes("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, Optional<byte[]>> value = a.value();

    assertThat(value.apply(empty), is(empty()));
  }

  @Test
  public void testValueOrUsesDefault() {
    BytesCellDefinition a = defineBytes("foo");

    Record<?> empty = mock(Record.class);

    byte[] array = new byte[128];
    Arrays.fill(array, (byte) 0x2a);
    Function<Record<?>, byte[]> value = a.valueOr(array);

    assertThat(value.apply(empty), is(array));
  }

  @Test
  public void testValueOrFailDoes() {
    BytesCellDefinition a = defineBytes("foo");

    Record<?> empty = mock(Record.class);

    Function<Record<?>, byte[]> value = a.valueOrFail();


    try {
      value.apply(empty);
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  @Test
  public void testValueExtracts() {
    BytesCellDefinition a = defineBytes("foo");

    byte[] one = {1};
    byte[] two = {2, 2};
    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(one));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(two));

    Function<Record<?>, Optional<byte[]>> value = a.value();

    assertThat(value.apply(oneRecord), is(of(one)));
    assertThat(value.apply(twoRecord), is(of(two)));
  }

  @Test
  public void testValueOrExtracts() {
    BytesCellDefinition a = defineBytes("foo");

    byte[] one = {1};
    byte[] two = {2, 2};
    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(one));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(two));

    Function<Record<?>, byte[]> value = a.valueOr(new byte[] {});

    assertThat(value.apply(oneRecord), is(one));
    assertThat(value.apply(twoRecord), is(two));
  }

  @Test
  public void testValueOrFailExtracts() {
    BytesCellDefinition a = defineBytes("foo");

    byte[] one = {1};
    byte[] two = {2, 2};
    Record<?> oneRecord = mock(Record.class);
    when(oneRecord.get(a)).thenReturn(of(one));
    Record<?> twoRecord = mock(Record.class);
    when(twoRecord.get(a)).thenReturn(of(two));

    Function<Record<?>, byte[]> value = a.valueOrFail();

    assertThat(value.apply(oneRecord), is(one));
    assertThat(value.apply(twoRecord), is(two));
  }
}
