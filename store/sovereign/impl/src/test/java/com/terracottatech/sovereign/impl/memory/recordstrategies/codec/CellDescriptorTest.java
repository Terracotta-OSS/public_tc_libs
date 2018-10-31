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

package com.terracottatech.sovereign.impl.memory.recordstrategies.codec;

import org.junit.Test;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.store.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class CellDescriptorTest {

  static final Set<Type<?>> NEVADA_TYPES;
  static {
    final Set<Type<?>> all = new HashSet<>();
    for (final Field field : Type.class.getDeclaredFields()) {
      final int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers)
          && Modifier.isPublic(modifiers)
          && Modifier.isFinal(modifiers)
          && field.getType().isAssignableFrom(Type.class))
        try {
          all.add((Type<?>)field.get(null));
        } catch (IllegalAccessException e) {
          throw new AssertionError(e);
        }
    }
    NEVADA_TYPES = Collections.unmodifiableSet(all);
  }

  @Test
  public void testBaseCtor() throws Exception {
    try {
      final CellDescriptor descriptor = new CellDescriptor(0, (SovereignType)null, 0, 0L);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    for (final SovereignType type : SovereignType.values()) {
      final CellDescriptor descriptor = new CellDescriptor(-1, type, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
      assertThat(descriptor.cellFlags, is(-1));
      assertThat(descriptor.valueType, is(type));
      assertThat(descriptor.nameDisplacement, is(0xDEADBEEF));
      assertThat(descriptor.valueDisplacement, is(0xCAFEBABEDEADBEEFL));
    }
  }

  @Test
  public void testTypeCtor() throws Exception {
    for (final Type<?> nevadaType : NEVADA_TYPES) {
      final CellDescriptor descriptor = new CellDescriptor(-1, nevadaType, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
      assertThat(descriptor.cellFlags, is(-1));
      assertThat(descriptor.valueType, is(SovereignType.forType(nevadaType)));
      assertThat(descriptor.nameDisplacement, is(0xDEADBEEF));
      assertThat(descriptor.valueDisplacement, is(0xCAFEBABEDEADBEEFL));
    }
  }

  @Test
  public void testPutSize() throws Exception {
    final CellDescriptor descriptor = new CellDescriptor(0x5AA5, SovereignType.BYTES, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    assertThat(buffer.position(), is(0));
    descriptor.put(buffer);
    assertThat(buffer.position(), is(CellDescriptor.serializedSize()));
  }

  @Test
  public void testPutGet() throws Exception {
    final CellDescriptor original = new CellDescriptor(0x5AA5, SovereignType.BYTES, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    assertThat(buffer.position(), is(0));
    original.put(buffer);
    assertThat(buffer.position(), is(CellDescriptor.serializedSize()));

    buffer.rewind();

    final CellDescriptor observed = new CellDescriptor(buffer);
    assertThat(observed.cellFlags, is(original.cellFlags));
    assertThat(observed.valueType, is(original.valueType));
    assertThat(observed.nameDisplacement, is(original.nameDisplacement));
    assertThat(observed.valueDisplacement, is(original.valueDisplacement));
  }

  @Test
  public void testPutOverflow() throws Exception {
    final CellDescriptor original = new CellDescriptor(0x5AA5, SovereignType.BYTES, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
    final ByteBuffer buffer = ByteBuffer.allocate(CellDescriptor.serializedSize() / 2);

    assertThat(buffer.position(), is(0));
    try {
      original.put(buffer);
      fail();
    } catch (BufferOverflowException e) {
      // expected
    }
  }

  @Test
  public void testGetUnderflow() throws Exception {
    final CellDescriptor original = new CellDescriptor(0x5AA5, SovereignType.BYTES, 0xDEADBEEF, 0xCAFEBABEDEADBEEFL);
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    assertThat(buffer.position(), is(0));
    original.put(buffer);
    assertThat(buffer.position(), is(CellDescriptor.serializedSize()));

    buffer.rewind();
    final ByteBuffer truncatedBuffer = buffer.duplicate();
    truncatedBuffer.limit(CellDescriptor.serializedSize() / 2);

    try {
      new CellDescriptor(truncatedBuffer);
      fail();
    } catch (BufferUnderflowException e) {
      // expected
    }
  }
}
