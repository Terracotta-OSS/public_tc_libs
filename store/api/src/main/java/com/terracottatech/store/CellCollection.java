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

import com.terracottatech.store.definition.CellDefinition;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * A collection of {@link Cell}s.
 * The iteration order is not defined for a {@code CellCollection} but may be specified
 * by an API method returning an instance.
 */
public interface CellCollection extends Collection<Cell<?>> {

  /**
   * Retrieve a cell by {@link CellDefinition}.  If this record contains a cell whose definition matches
   * the supplied definition (in both name and type) then that cell's value is
   * wrapped in an {@code Optional} and returned.  If this record contains no
   * matching cell then an empty {@code Optional} is returned.
   *
   * @param <T> the cell JDK type
   * @param cellDefinition cell to retrieve
   * @return An {@code Optional} with the cell value or an empty {@code Optional}
   *         if the record contains no matching cell
   *
   * @throws NullPointerException if {@code cellDefinition} is {@code null}
   */
  default <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    Objects.requireNonNull(cellDefinition);
    return stream().filter(c -> c.definition().equals(cellDefinition))
            .findAny().map(Cell::value).map(cellDefinition.type().getJDKType()::cast);
  }

  /**
   * Retrieve a cell by name.  If this record contains a cell whose definition matches
   * the supplied name then that cell's value is
   * wrapped in an {@code Optional} and returned.  If this record contains no
   * matching cell then an empty {@code Optional} is returned.
   *
   * @param name cell to retrieve
   * @return An {@code Optional} with the cell value or an empty {@code Optional}
   *         if the record contains no matching cell
   *
   * @throws NullPointerException if {@code name} is {@code null}
   */
  default Optional<?> get(String name) {
    Objects.requireNonNull(name);
    return stream().filter(c -> c.definition().name().equals(name))
        .findAny().map(Cell::value);
  }
}
