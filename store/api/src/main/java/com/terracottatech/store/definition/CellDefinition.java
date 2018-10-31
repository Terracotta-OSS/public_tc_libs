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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.function.BuildableOptionalFunction;

import java.util.NoSuchElementException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A {@code {name, type}} tuple that identifies a cell.  Cells within a record
 * must have definitions with unique names.  Cells however are not considered
 * related for the purposes of cell to cell comparison when performing
 * computation operations unless the two cell definitions are identical in both
 * name and type.
 *
 * @param <T> the associated JDK type
 * @author Alex Snaps
 */
public interface CellDefinition<T> {

  /**
   * Returns the name of this definition.
   *
   * @return definition name
   */
  String name();

  /**
   * Returns the type of this definition.
   *
   * @return definition type
   */
  Type<T> type();

  /**
   * Creates a new cell binding this definition to the given value.
   *
   * @param value value for the cell
   * @return a cell
   */
  Cell<T> newCell(T value);

  /**
   * Returns a function that extracts the value of this cell from a record.
   *
   * @return a cell extracting function
   */
  default BuildableOptionalFunction<Record<?>, T> value() {
    return Functions.extractPojo(this);
  }

  /**
   * Returns a function that extracts the value of this cell from a record or
   * uses the given default if the cell is absent.
   *
   * @param otherwise default value for absent cells
   * @return a cell extracting function
   */
  default BuildableFunction<Record<?>, T> valueOr(T otherwise) {
    Objects.requireNonNull(otherwise, "Otherwise value must be non-null");
    return Functions.valueOr(this, otherwise);
  }

  /**
   * Returns a function that extracts the value of this cell from a record or
   * throws a {@link NoSuchElementException} if the cell is absent.
   *
   * @return a cell extracting function
   */
  default BuildableFunction<Record<?>, T> valueOrFail() {
    return Functions.valueOrFail(this);
  }

  /**
   * Returns a predicate that tests for the presence of this cell on a record.
   *
   * @return a cell existence predicate
   */
  default BuildablePredicate<Record<?>> exists() {
    return Functions.cellDefinitionExists(this);
  }

  /**
   * Returns a {@link Type#BOOL boolean} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a boolean cell definition
   */
  static BoolCellDefinition defineBool(String name) {
    return (BoolCellDefinition) define(name, Type.BOOL);
  }

  /**
   * Returns a {@link Type#CHAR character} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a character cell definition
   */
  static CharCellDefinition defineChar(String name) {
    return (CharCellDefinition) define(name, Type.CHAR);
  }

  /**
   * Returns an {@link Type#INT integer} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return an integer cell definition
   */
  static IntCellDefinition defineInt(String name) {
    return (IntCellDefinition) define(name, Type.INT);
  }

  /**
   * Returns a {@link Type#LONG long} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a long cell definition
   */
  static LongCellDefinition defineLong(String name) {
    return (LongCellDefinition) define(name, Type.LONG);
  }

  /**
   * Returns a {@link Type#DOUBLE double} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a double cell definition
   */
  static DoubleCellDefinition defineDouble(String name) {
    return (DoubleCellDefinition) define(name, Type.DOUBLE);
  }

  /**
   * Returns a {@link Type#STRING string} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a string cell definition
   */
  static StringCellDefinition defineString(String name) {
    return (StringCellDefinition) define(name, Type.STRING);
  }

  /**
   * Returns a {@link Type#BYTES byte array} cell definition with the given {@code name}.
   *
   * @param name cell definition name
   * @return a byte array cell definition
   */
  static BytesCellDefinition defineBytes(String name) {
    return (BytesCellDefinition) define(name, Type.BYTES);
  }

  /**
   * Returns a cell definition with the given {@code type} and {@code name}.
   *
   * @param <T> cell definition JDK type
   * @param name cell definition name
   * @param type cell definition type
   *
   * @return a cell definition
   */
  static <T> CellDefinition<T> define(String name, Type<T> type) {
    requireNonNull(name, "Definition name must be non-null");
    requireNonNull(type, "Definition type must be non-null");
    return Impl.intern(name, type);
  }

  /**
   * Internal interned definition implementation.
   *
   * @param <T> definition JDK type
   */
  abstract class Impl<T> implements CellDefinition<T> {

    private static final DefinitionInterner DEFINITIONS = new DefinitionInterner(Impl::create);

    private static <T> CellDefinition<T> intern(String name, Type<T> type) {
      return DEFINITIONS.intern(name, type);
    }

    @SuppressWarnings("unchecked")
    private static <T> CellDefinition<T> create(String name, Type<T> type) {
      switch (type.asEnum()) {
        case BOOL:
          return (CellDefinition<T>) new BoolImpl(name);
        case BYTES:
          return (CellDefinition<T>) new BytesImpl(name);
        case CHAR:
          return (CellDefinition<T>) new CharImpl(name);
        case DOUBLE:
          return (CellDefinition<T>) new DoubleImpl(name);
        case INT:
          return (CellDefinition<T>) new IntImpl(name);
        case LONG:
          return (CellDefinition<T>) new LongImpl(name);
        case STRING:
          return (CellDefinition<T>) new StringImpl(name);
      }
      throw new AssertionError("Unexpected type: " + type);
    }

    private final String name;
    private final Type<T> type;

    Impl(String name, Type<T> type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public final String name() {
      return name;
    }

    @Override
    public final Type<T> type() {
      return type;
    }

    @Override
    public final Cell<T> newCell(T value) {
      requireNonNull(value, "Cell value must be non-null");
      return new CellImpl<>(this, value);
    }

    @Override
    public final boolean equals(Object obj) {
      if (obj instanceof CellDefinition<?>) {
        CellDefinition<?> other = (CellDefinition<?>) obj;
        return name().equals(other.name()) && type().equals(other.type());
      } else {
        return false;
      }
    }

    @Override
    public final int hashCode() {
      return name().hashCode() + 31 * type.hashCode();
    }

    @Override
    public String toString() {
      return "CellDefinition[name='" + name() + "' type='" + type() + "']";
    }

    private static class BoolImpl extends Impl<Boolean> implements BoolCellDefinition {
      BoolImpl(String name) {
        super(name, Type.BOOL);
      }
    }

    private static class CharImpl extends Impl<Character> implements CharCellDefinition {
      CharImpl(String name) {
        super(name, Type.CHAR);
      }
    }

    private static class IntImpl extends Impl<Integer> implements IntCellDefinition {
      IntImpl(String name) {
        super(name, Type.INT);
      }
    }

    private static class LongImpl extends Impl<Long> implements LongCellDefinition {
      LongImpl(String name) {
        super(name, Type.LONG);
      }
    }

    private static class DoubleImpl extends Impl<Double> implements DoubleCellDefinition {
      DoubleImpl(String name) {
        super(name, Type.DOUBLE);
      }
    }

    private static class StringImpl extends Impl<String> implements StringCellDefinition {
      StringImpl(String name) {
        super(name, Type.STRING);
      }
    }

    private static class BytesImpl extends Impl<byte[]> implements BytesCellDefinition {
      BytesImpl(String name) {
        super(name, Type.BYTES);
      }
    }
  }

  class CellImpl<T> implements Cell<T> {

    private final CellDefinition<T> definition;
    private final T value;

    private CellImpl(CellDefinition<T> definition, T value) {
      this.definition = definition;
      this.value = value;
    }

    @Override
    public CellDefinition<T> definition() {
      return definition;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Cell<?>) {
        Cell<?> other = (Cell<?>) obj;
        return definition().equals(other.definition()) && Type.equals(value(), other.value());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return definition().hashCode() + 31 * value().hashCode();
    }

    @Override
    public String toString() {
      return "Cell[definition=" + definition() + " value='" + value() + "']";
    }
  }
}
