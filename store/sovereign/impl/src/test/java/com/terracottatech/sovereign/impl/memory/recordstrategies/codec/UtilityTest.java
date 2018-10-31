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
import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Clifford W. Johnson
 */
public class UtilityTest {

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

  private final Map<Type<?>, Object> typeObjects;
  {
    final Map<Type<?>, Object> map = new HashMap<>();
    map.put(Type.BOOL, Boolean.TRUE);
    map.put(Type.BYTES, new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF});
    map.put(Type.CHAR, 'a');
    map.put(Type.DOUBLE, Math.PI);
    map.put(Type.INT, 8675309);
    map.put(Type.LONG, 19173512000000L);
    map.put(Type.STRING, "cafebabe");

    assertThat(map.size(), is(NEVADA_TYPES.size()));

    typeObjects = Collections.unmodifiableMap(map);
  }

  @Test
  public void testGetTypeCell() throws Exception {
    for (final Map.Entry<Type<?>, Object> entry : typeObjects.entrySet()) {
      assertThat(Utility.getType(Cell.cell("test", entry.getValue())), is(SovereignType.forType(entry.getKey())));
    }
  }

  @Test
  public void testGetTypeType() throws Exception {
    for (final Type<?> type : NEVADA_TYPES) {
      assertThat(Utility.getType(type), is(SovereignType.forType(type)));
    }
  }

  @Test
  public void testGetTypeClass() throws Exception {
    for (final Type<?> type : NEVADA_TYPES) {
      assertThat(Utility.getType(type.getJDKType()), is(SovereignType.forType(type)));
    }

    try {
      Utility.getType(BigInteger.class);
      fail();
    } catch (EncodingException e) {
      // expected
    }
  }
}
