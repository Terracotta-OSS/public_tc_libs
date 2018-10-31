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

import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Provides a number of methods producing functions for manipulating Sovereign
 * {@link com.terracottatech.store.Record Record} and {@link com.terracottatech.store.Cell Cell}
 * instances.
 *
 * <h3>Sample Usage</h3>
 * <h4>Example 1</h4>
 * <pre>{@code
 * final Dataset<Integer> dataset = ...
 * final CellDefinition<String> firstCell = cellDefinition("first", String.class);
 * final CellDefinition<byte[]> secondCell = cellDefinition("second", byte[].class);
 *
 * dataset.add(1, cell(firstCell, "value"), cell(secondCell, new byte[] {'a', 'b', 'c', 'd'}));
 * dataset.add(2, cell(firstCell, "value2"), cell(secondCell, new byte[] {'d', 'e', 'a', 'd', 'b', 'e', 'e', 'f'}));
 *
 * dataset.applyMutation(2, alterRecord(assign(secondCell, new byte[] { 'm', 'a', 'd', ' ', 'c', 'o', 'w' })));
 * }</pre>
 *
 * <h4>Example 2</h4>
 * <pre>{@code
 * final Dataset<Integer> dataset = ...
 * final CellDefinition<String> tag = cellDefinition("tag", Type.STRING);
 * final CellDefinition<Integer> count = cellDefinition("count", Type.INT);
 * final CellDefinition<Integer> series = cellDefinition("series", Type.INT);
 *
 * dataset.add(1, tag.newValue("CD"), count.newValue(1), series.newValue(1));
 *
 * dataset.applyMutation(1, alterRecord(assign(tag, "CJ"), remove(series)));
 * }</pre>
 *
 * @author Clifford W. Johnson
 */
// Due to a generics bug in javac, this class fails compilation using Java 1.8.0_25 but works with Java 1.8.0_31
@SuppressWarnings("UnusedDeclaration")
public final class RecordFunctions {

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private RecordFunctions() {
    throw new AssertionError("Not supported");
  }

  public static String btos(byte[] ba) {
    return new String(ba, Charset.forName("UTF-8"));
  }

  public static byte[] stob(String s) {
    return s.getBytes(Charset.forName("UTF-8"));
  }

  /**
   * Returns a {@code Function} over a {@link com.terracottatech.store.Record Record} returning the
   * collection of {@code Cell}s resulting from the application of each of the
   * {@link java.util.function.Function Function} instances provided to the {@code Cell}s held in the
   * {@code Record}.
   *
   * @param alterations an ordered sequence of {@code Function}s to apply to each {@code Cell} in
   *    each {@code Record} supplied to the {@link java.util.function.Function#apply(Object) apply} method of the
   *    returned {@code Function} -- every {@code Function} in {@code alterations} is applied, in sequence,
   *    to every {@code Cell} in the {@code Record}.  If the {@code Function} returns {@code null}, the
   *    {@code Cell} is left unaltered; if the {@code Function} returns a {@code Cell} for which the
   *    {@link com.terracottatech.store.Cell#value() value} method returns {@code null}, the {@code Cell}
   *    is <i>removed</i> from the {@code Record}.  Each alteration is provided the {@code Cell} from the
   *    record -- previous alterations, if any, are not observable.  Only the {@code Cell} produced by the
   *    last alteration is returned.
   * @param <K> the key type of the {@code Record}
   *
   * @return a {@code Function} returning a new collection of {@code Cell}s resulting from applying
   *    each {@code Function} in {@code alterations} to the {@code Cell}s contained in the {@code Record}
   *    passed to the {@link java.util.function.Function#apply(Object) Function.apply} method.
   *
   * @see #assign(CellDefinition, Object)
   * @see #remove(CellDefinition)
   */
  @SafeVarargs
  public static <K extends Comparable<K>>
  Function<Record<K>, Iterable<Cell<?>>> alterRecord(final Function<Cell<?>, Cell<?>>... alterations) {
    if (alterations.length == 0) {
      return r -> r;
    }
    return r -> {
      final LinkedHashMap<String, Cell<?>> result = new LinkedHashMap<>();
      r.forEach(c -> {
        Cell<?> newCell = c;
        for (Function<Cell<?>, Cell<?>> alteration : alterations) {
          final Cell<?> computedCell = alteration.apply(c);
          if (computedCell != null) {
            newCell = computedCell;
          }
        }
        if (newCell.value() != null) {
          if (!c.definition().name().equals(newCell.definition().name())) {
            result.put(c.definition().name(), c);
          }
          result.put(newCell.definition().name(), newCell);
        }
      });
      return result.values();
    };
  }

  /**
   * Returns a {@code Function} over a {@link com.terracottatech.store.Cell Cell} returning a new cell
   * computed from the {@code Cell} input to the {@link Function#apply(Object) Function.apply} method if
   * the input {@code Cell} matches the {@link CellDefinition CellDefinition}
   * provided.
   *
   * @param definition the {@code CellDefinition} for the {@code Cell} triggering the function
   * @param expression the {@code Function} generating a new {@code Cell} from an input {@code Cell}
   *    matching {@code definition}
   * @param <T> the value type of the input {@code Cell}
   *
   * @return a function returning a new {@code Cell} resulting from
   *    {@link java.util.function.Function#identity() expression.apply} if the input {@code Cell} matches
   *    {@code definition}; if the input {@code Cell{ does not match {@code definition}, {@code null} is
   *    returned indicating the {@code Cell} is not affected
   */
  // TODO: IntelliJ accommodation - Return should be Function<Cell<T>, Cell<?>>
  @SuppressWarnings("unchecked")
  public static <T> Function<Cell<?>, Cell<?>> compute(final CellDefinition<T> definition, final Function<Cell<T>, Cell<?>> expression) {
    requireNonNull(definition, "definition");
    requireNonNull(expression, "expression");

    return currentCell -> {
      if (definition.name().equals(currentCell.definition().name())) {
        return expression.apply((Cell<T>)currentCell);
      } else {
        // Does not apply
        return null;
      }
    };
  }

  /**
   * Returns a {@code Function} over a {@link com.terracottatech.store.Cell Cell} returning a new cell (with
   * an updated value) if the {@code Cell} provided to {@link java.util.function.Function#apply(Object) Function.apply}
   * matches the {@link CellDefinition CellDefinition} provided.
   *
   * @param definition the {@code CellDefinition} for the {@code Cell} to update
   * @param value the value to set in the new {@code Cell}
   * @param <T> the value type of the {@code Cell}
   *
   * @return a {@code Function} returning a new {@code Cell} containing {@code value} if {@code definition} matches
   *    the cell provided to {@link java.util.function.Function#apply(Object) Function.apply}; if the cell
   *    definitions do not match, {@code null} is returned indicating the {@code Cell} is not affected
   *
   * @see #alterRecord(java.util.function.Function[])
   */
  // TODO: IntelliJ accommodation - Return should be Function<Cell<T>, Cell<T>>
  public static <T> Function<Cell<?>, Cell<?>> assign(final CellDefinition<T> definition, final T value) {
    requireNonNull(definition, "definition");
    requireNonNull(value, "value");

    return currentCell -> {
      if (definition.name().equals(currentCell.definition().name())) {
        return definition.newCell(value);
      } else {
        // Does not apply to Cell
        return null;
      }
    };
  }

  /**
   * Returns a {@code Function} over a {@link com.terracottatech.store.Cell Cell} returning a <i>marker</i>
   * {@code Cell} instance having a {@code null} value if the {@code Cell} provided to
   * {@link java.util.function.Function#apply(Object) Function.apply} matches the
   * {@link CellDefinition CellDefinition} provided.  To
   * {@link #alterRecord(java.util.function.Function[]) alterRecord}, this indicates the cell is to be removed
   * from the record.
   *
   * @param definition the {@code CellDefinition} for the cell to remove
   * @param <T> the value type of the {@code Cell}
   *
   * @return a {@code Function} returning a <i>marker</i> {@code Cell} with a {@code null} value if {@code definition}
   *    matches the cell provided to {@link java.util.function.Function#apply(Object) Function.apply}; if the cell
   *    definitions do not match, {@code null} indicating the {@code Cell} is not affected
   *
   * @see #alterRecord(java.util.function.Function[])
   */
  // TODO: IntelliJ accommodation - Return should be Function<Cell<T>, Cell<T>>
  public static <T> Function<Cell<?>, Cell<?>> remove(final CellDefinition<T> definition) {
    requireNonNull(definition, "definition");

    return currentCell -> {
      if (definition.name().equals(currentCell.definition().name())) {
        return new Cell<T>() {
          @Override
          public CellDefinition<T> definition() {
            return definition;
          }

          @Override
          public T value() {
            // Indicate the Cell should be removed
            return null;
          }
        };
      } else {
        // Does not apply to Cell
        return null;
      }
    };
  }

  /**
   * Writes a formatted {@code Record} to the {@code PrintStream} specified.
   *
   * @param out the target {@code PrintStream}
   * @param prefix a string written before the formatted {@code Record}
   * @param record the {@code Record} to write
   */
  public static void printRecord(final PrintStream out, final String prefix, final Record<?> record) {
    requireNonNull(out, "out");
    requireNonNull(record, "record");
    out.format("%s: key='%s'", prefix, record.getKey());
    if (record instanceof SovereignRecord<?>) {
      out.format(" [timeReference]=%s", ((SovereignRecord<?>) record).getTimeReference());
    }
    if (record instanceof SovereignPersistentRecord) {
      out.format(" [MSN]=%d", ((SovereignPersistentRecord)record).getMSN());
    }
    record.forEach(cell -> out.format(", [%s]=%s", cell.definition().name(), formatCellValue(cell)));
    out.println();
  }

  /**
   * Formats the value of a {@link Cell Cell} for dumping purposes.
   *
   * @param cell the {@code Cell} to format
   * @param <T> the value type of {@code cell}
   *
   * @return the formatted, {@code String} value of {@code cell}
   */
  private static <T> CharSequence formatCellValue(final Cell<T> cell) {
    final Type<T> valueType = cell.definition().type();

    if (Type.BYTES.equals(valueType)) {
      return Arrays.toString((byte[])cell.value());
    }

    return cell.value().toString();
  }
}
