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

package com.terracottatech.sovereign.impl;

import org.junit.Test;

import com.terracottatech.store.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class SovereignTypeTest {

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

  /**
   * Ensures that {@link SovereignType} has a mapping for each {@link Type} value.
   */
  @Test
  public void testGetNevadaType() throws Exception {
    final EnumSet<SovereignType> normalTypes = EnumSet.allOf(SovereignType.class);

    final Set<Type<?>> observedTypes = new HashSet<>();
    for (final SovereignType sovereignType : normalTypes) {
      observedTypes.add(sovereignType.getNevadaType());
    }

    assertThat(observedTypes, is(equalTo(NEVADA_TYPES)));
  }

  /**
   * Ensures that {@link SovereignType} has the same JDK type mapping as the associated {@link Type} value.
   */
  @Test
  public void testGetJDKType() throws Exception {
    final EnumSet<SovereignType> normalTypes = EnumSet.allOf(SovereignType.class);

    for (final SovereignType sovereignType : normalTypes) {
      assertThat(sovereignType.getJDKType(), is(equalTo(sovereignType.getNevadaType().getJDKType())));
    }
  }

  /**
   * Ensures that {@link SovereignType} matches the Nevada and JDK type associations.
   */
  @Test
  public void testForJDKType() throws Exception {
    for (final Type<?> nevadaType : NEVADA_TYPES) {
      assertThat(SovereignType.forJDKType(nevadaType.getJDKType()), is(equalTo(SovereignType.forType(nevadaType))));
    }

    // Unsupported type
    assertThat(SovereignType.forJDKType(BigInteger.class), is(nullValue()));
  }

  /**
   * Ensures that {@link SovereignType} Nevada type lookup is symmetric.
   */
  @Test
  public void testForType() throws Exception {
    for (final Type<?> nevadaType : NEVADA_TYPES) {
      assertThat(SovereignType.forType(nevadaType).getNevadaType(), is(nevadaType));
    }
  }
}
