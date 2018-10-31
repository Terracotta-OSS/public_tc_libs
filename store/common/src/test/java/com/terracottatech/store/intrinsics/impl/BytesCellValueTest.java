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
package com.terracottatech.store.intrinsics.impl;

import org.junit.Test;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.Intrinsic;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Basic tests for {@link CellValue CellValue<byte[]>}.
 */
public class BytesCellValueTest {

  private final byte[] deafBeef = new byte[] {(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF };
  private final BytesCellDefinition bytesCell = CellDefinition.defineBytes("bytesCell");
  private final Record<?> deadBeefRecord = new TestRecord<>("deadBeef", singleton(bytesCell.newCell(deafBeef)));
  private final Record<?> nilRecord = new TestRecord<>("nilRecord", emptySet());


  @Test
  public void testIs() throws Exception {
    Predicate<Record<?>> predicate = new CellValue<>(bytesCell, new byte[0]).is(deafBeef.clone());
    assertThat(predicate, is(instanceOf(Intrinsic.class)));
    assertTrue(predicate.test(deadBeefRecord));
    assertFalse(predicate.negate().test(deadBeefRecord));

    assertFalse(predicate.test(nilRecord));
    assertTrue(predicate.negate().test(nilRecord));

    Predicate<Record<?>> noDefaultPredicate = new CellValue<>(bytesCell, null).is(deafBeef.clone());
    assertThat(noDefaultPredicate, is(instanceOf(Intrinsic.class)));
    assertThrows(() -> noDefaultPredicate.test(nilRecord), NoSuchElementException.class);
    assertThrows(() -> noDefaultPredicate.negate().test(nilRecord), NoSuchElementException.class);
  }

  @SuppressWarnings("Duplicates")
  private static <T extends Exception> void assertThrows(Procedure proc, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  @FunctionalInterface
  private interface Procedure {
    void invoke();
  }
}
