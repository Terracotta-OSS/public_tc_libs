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
package com.terracottatech.store.server;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;

import java.util.LinkedHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class Utils {

  private Utils() {

  }

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

  @SuppressWarnings("unchecked")
  public static <T> Function<Cell<?>, Cell<?>> compute(final CellDefinition<T> definition, final Function<Cell<T>, Cell<?>> expression) {
    requireNonNull(definition, "definition");
    requireNonNull(expression, "expression");

    return currentCell -> {
      if (definition.name().equals(currentCell.definition().name())) {
        return expression.apply((Cell<T>) currentCell);
      } else {
        // Does not apply
        return null;
      }
    };
  }
}
