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
package com.terracottatech.store;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The representation of a Terracotta Store type.
 *
 * @author Alex Snaps
 */
public final class Type<T> {

  public enum Enum {
    BOOL,
    CHAR,
    INT,
    LONG,
    DOUBLE,
    STRING,
    BYTES;

    Type<?> asType() {
      return fromOrdinal(ordinal());
    }
  }

  /**
   * The {@link Boolean} type.
   */
  public static final Type<Boolean>      BOOL    = new Type<>(Enum.BOOL, Boolean.class);

  /**
   * The {@link Character} type.
   */
  public static final Type<Character>    CHAR    = new Type<>(Enum.CHAR, Character.class);

  /**
   * The {@link Integer} type.
   */
  public static final Type<Integer>      INT     = new Type<>(Enum.INT, Integer.class);

  /**
   * The {@link Long} type.
   */
  public static final Type<Long>         LONG    = new Type<>(Enum.LONG, Long.class);

  /**
   * The {@link Double} type.
   */
  public static final Type<Double>       DOUBLE  = new Type<>(Enum.DOUBLE, Double.class);

  /**
   * The {@link String} type.
   */
  public static final Type<String> STRING  = new Type<>(Enum.STRING, String.class);

  /**
   * The binary type.
   */
  public static final Type<byte[]>       BYTES   = new Type<>(Enum.BYTES, byte[].class);

  private final Class<T> type;
  private final Enum enumeration;

  private Type(Enum enumeration, Class<T> clazz) {
    this.enumeration = enumeration;
    this.type = clazz;
  }

  @Override
  public String toString() {
    return "Type<" + getJDKType().getSimpleName() + ">";
  }

  /**
   * Returns the corresponding JDK type for this Terracotta Store type.
   *
   * @return the JDK type
   */
  public Class<T> getJDKType() {
    return type;
  }

  public Enum asEnum() {
    return enumeration;
  }

  /**
   * Return the Terracotta Store type corresponding to the supplied JDK type.
   * <p>
   * If there is no type corresponding to the supplied JDK type then
   * {@code null} will be returned.
   *
   * @param <T> JDK type corresponding to the Terracotta Store type
   * @param clazz a JDK type
   * @return the corresponding Terracotta Store type
   */
  @SuppressWarnings("unchecked")
  public static <T> Type<T> forJdkType(Class<? extends T> clazz) {
    for (Type<?> t : allTypes) {
      if(t.getJDKType().isAssignableFrom(clazz)) {
        return (Type<T>) t;
      }
    }
    return null;
  }

  private static final Set<Type<?>> allTypes;
  private static final Type<?>[] allTypeArray;

  static {
    Set<Type<?>> all = new HashSet<>();
    for (Field field : Type.class.getDeclaredFields()) {
      if (field.getType().isAssignableFrom(Type.class) && Modifier.isPublic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())) {
        try {
          all.add((Type<?>) field.get(null));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
    allTypes = Collections.unmodifiableSet(all);
    // verify contiguous
    if (!allTypes.stream().map(Type::asEnum).collect(Collectors.toSet()).equals(EnumSet.allOf(Enum.class))) {
      throw new AssertionError("Bad Type ordinal seen");
    }
    allTypeArray = new Type<?>[allTypes.size()];
    for (Type<?> t : allTypes) {
      allTypeArray[t.asEnum().ordinal()] = t;
    }
  }

  public static Type<?> fromOrdinal(int ordinal) {
    return allTypeArray[ordinal];
  }

  /**
   * Compares to cell values for equality.
   * <p>
   * Cell value equality follows conventional Java behavior, except for {@link Type#BYTES} cells which are compared using
   * {@link java.util.Arrays#equals(byte[], byte[])}.
   *
   * @param a first cell value
   * @param b second cell value
   * @return {@code true} if the cell values are equal
   */
  public static boolean equals(Object a, Object b) {
    requireNonNull(a);
    requireNonNull(b);
    Type<?> aType = forJdkType(a.getClass());
    Type<?> bType = forJdkType(b.getClass());
    requireNonNull(aType);
    requireNonNull(bType);

    if (aType.equals(bType)) {
      if (Type.BYTES.equals(aType)) {
        return Arrays.equals((byte[]) a, (byte[]) b);
      } else {
        return a.equals(b);
      }
    } else {
      return false;
    }
  }
}
