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

package com.terracottatech.sovereign.testsupport;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Generates {@link VersionedRecord} instances using pseudo-random content.  The generated records
 * have zero or more {@link Cell} instances randomly selected from the types declared in
 * {@link SovereignType}.
 *
 * @param <K> the key type for the records
 * @param <Z> the {@link TimeReference} type for the records
 */
public final class RecordGenerator<K extends Comparable<K>, Z extends TimeReference<Z>> {
  private final Random rnd = new Random(8675309L);

  private final Class<K> keyType;
  private final Supplier<K> keyGenerator;
  private final TimeReferenceGenerator<Z> timeGenerator;

  private final Set<SovereignType> testTypes;
  private final int testTypeMaskBound;
  {
    EnumSet<SovereignType> liveTypes = EnumSet.allOf(SovereignType.class);
    this.testTypes = Collections.unmodifiableSet(liveTypes);

    int typeMask = 0;
    for (final SovereignType type : this.testTypes) {
      typeMask |= 1 << type.ordinal();
    }
    this.testTypeMaskBound = clp2(typeMask);
  }

  private long msnSource = Long.MIN_VALUE;

  /**
   * Creates a new {@code RecordGenerator} using the key {@code Supplier} and
   * {@code TimeReferenceGenerator} supplied.
   *
   * @param keyType the type of the record key
   * @param timeGenerator the {@code TimeReferenceGenerator} to use for {@link TimeReference} generation
   */
  public RecordGenerator(final Class<K> keyType, final TimeReferenceGenerator<Z> timeGenerator) {
    this.keyType = Objects.requireNonNull(keyType, "keyType");
    this.timeGenerator = Objects.requireNonNull(timeGenerator, "timeGenerator");
    this.keyGenerator = () -> this.makeKey(keyType);
  }

  /**
   * Gets the key type produced by this {@code RecordGenerator}.
   *
   * @return the key type
   */
  public Class<K> getKeyType() {
    return keyType;
  }

  /**
   * Generates a {@code VersionedRecord} instance with <i>up to</i> the specified number of
   * versions.
   *
   * @param versionCountBound the upper limit of the number of versions for the generated record;
   *                          must be at least one
   *
   * @return a new {@code VersionedRecord} with pseudo-random content
   */
  public VersionedRecord<K> makeRecord(final int versionCountBound) {
    if (versionCountBound < 1) {
      throw new IllegalArgumentException();
    }

    final K key = this.keyGenerator.get();

    final VersionedRecord<K> parent = new VersionedRecord<>();
    final int versionCount = 1 + this.rnd.nextInt(versionCountBound);

    final LinkedList<SingleRecord<K>> versions = new LinkedList<>();
    for (int i = 0; i < versionCount; i++) {
      final SingleRecord<K> version =
          new SingleRecord<>(parent, key, this.timeGenerator.get(), ++this.msnSource, this.makeCells());
      versions.addFirst(version);
    }
    parent.elements().addAll(versions);

    return parent;
  }

  /**
   * Generates a collection of {@code Cell} instances with pseudo-random content.  The collection
   * of cells generated is also random.
   *
   * @return a new {@code Collection} of cells
   */
  private Collection<Cell<?>> makeCells() {
    final int typeMask = this.rnd.nextInt(this.testTypeMaskBound);

    final List<Cell<?>> cells = new ArrayList<>(this.testTypes.size());
    for (final SovereignType type : this.testTypes) {
      if ((typeMask & type.ordinal()) != 0) {
        cells.add(this.makeCell(type.getJDKType()));
      }
    }

    return cells;
  }

  /**
   * Generates a new {@code Cell} with pseudo-random content.  The cell name is the same as its type.
   *
   * @param type the cell value type
   * @param <V> the cell value type
   *
   * @return a new {@code Cell} with random content
   */
  private <V> Cell<V> makeCell(final Class<V> type) {
    final SovereignType sovereignType = SovereignType.forJDKType(type);
    if (sovereignType == null) {
      throw new IllegalArgumentException();
    }
    return Cell.cell(sovereignType.name().toLowerCase(), this.makeValue(type));
  }

  /**
   * Generates a new key value.  For {@code String} keys, a 32,766 character value is generated.
   *
   * @param keyType the key type
   *
   * @return a new key value
   */
  @SuppressWarnings("unchecked")
  private K makeKey(final Class<K> keyType) {
    final SovereignType sovereignType = SovereignType.forJDKType(keyType);
    if (sovereignType == null) {
      throw new IllegalArgumentException();
    }

    switch (sovereignType) {
      case BOOLEAN:
      case CHAR:
      case INT:
      case LONG:
      case DOUBLE:
        return this.makeValue(keyType);

      case STRING:
        return (K)this.makeString(32766);     // unchecked

      default:
        throw new IllegalArgumentException("Type " + sovereignType + " not supported as a key");
    }
  }

  /**
   * Generates a typed value.
   *
   * @param type the value type
   * @param <V> the value type
   *
   * @return a new value
   */
  @SuppressWarnings("unchecked")
  private <V> V makeValue(final Class<V> type) {
    final SovereignType sovereignType = SovereignType.forJDKType(type);
    if (sovereignType == null) {
      throw new IllegalArgumentException();
    }

    final Object value;
    switch (sovereignType) {
      case BOOLEAN:
        value = rnd.nextBoolean();
        break;
      case CHAR:
        value = Character.toChars(rnd.nextInt(Character.MAX_CODE_POINT))[0];
        break;
      case STRING:
        value = this.randomString();
        break;
      case INT:
        value = rnd.nextInt();
        break;
      case LONG:
        value = rnd.nextLong();
        break;
      case DOUBLE:
        value = rnd.nextDouble();
        break;
      case BYTES: {
        final byte[] bytes = new byte[rnd.nextInt(32768)];
        rnd.nextBytes(bytes);
        value = bytes;
        break;
      }
      default:
        throw new IllegalArgumentException();
    }

    return (V)value;      // unchecked
  }

  /**
   * Generates and returns a pseudo-random {@code String} value.
   *
   * @return a random string
   */
  private String randomString() {
    final int length;
    if (this.rnd.nextBoolean()) {
      length = this.rnd.nextInt(1024);
    } else {
      length = this.rnd.nextInt(32767);
    }

    return this.makeString(length);
  }

  /**
   * Generates a pseudo-random key of the length indicated.
   *
   * @param length the desired length
   *
   * @return a new {@code String} with pseudo-random content
   */
  public String makeString(final int length) {
    final StringBuilder sb = new StringBuilder(length);
    final int capacity = sb.capacity();
    while (sb.length() < capacity) {
      sb.appendCodePoint(this.rnd.nextInt(1 + 0x07FF));
    }
    return sb.toString();
  }

  /**
   * Rounds up to the next power of 2.  The algorithm works for non-negative integers.
   * @param i the number for which the next power of 2 is desired
   * @return the power of 2 greater than or equal to {@code i}
   * @see <cite>Hacker's Delight, Chapter 3, Figure 3-3</cite>
   */
  private static int clp2(int i) {
    i -= 1;
    i |= i >> 1;
    i |= i >> 2;
    i |= i >> 4;
    i |= i >> 16;
    return i + 1;
  }
}
