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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

/**
 * A collection of {@link Cell}s having no more than one cell having a given name.
 * The iteration order is not defined for a {@code CellSet} but may be specified
 * by an API method returning an instance.
 * <p>
 * Null elements are not permitted in a {@code CellSet}.
 *
 * @see CellDefinition#name()
 * @see Record
 */
public final class CellSet extends AbstractSet<Cell<?>> implements CellCollection {

  private final Map<String, Cell<?>> cellsByName = new ConcurrentHashMap<>(1, 0.75F, 1);

  /**
   * Creates an new, mutable empty {@code CellSet} instance.
   */
  public CellSet() {}

  /**
   * Creates a new, mutable {@code CellSet} instance containing the {@link Cell}s from the specified
   * {@code Iterable} in the order returned by the iterator.  If more than one cell with the
   * same name is presented from the iterable, only the <i>last</i> like-named cell, as observed by the
   * iterator, is kept.
   *
   * @param cells the {@code Iterable} over {@code Cell} instances from which a new {@code CellSet}
   *              is created
   *
   * @throws NullPointerException if {@code cells} is {@code null} or {@code cells} contains a {@code null} element
   */
  public CellSet(Iterable<Cell<?>> cells) {
    this();
    cells.forEach(c -> cellsByName.put(c.definition().name(), c));
  }

  /**
   * Creates a new, mutable {@code CellSet} instance containing the specified {@link Cell} instances.
   * If more than one cell with the same name is present in the collection, only the <i>last</i> like-named cell
   * is kept.
   *
   * @param cells the {@code Cell} instances for the new {@code CellSet}
   *
   * @return a new, mutable {@code CellSet} containing the specified cells
   *
   * @throws NullPointerException if {@code cells} contains a {@code null}
   */
  public static CellSet of(Cell<?>... cells) {
    return new CellSet(Arrays.asList(cells));
  }

  @Override
  public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
    return ofNullable(cellsByName.get(cellDefinition.name())).filter(c -> cellDefinition.equals(c.definition()))
            .map(Cell::value).map(cellDefinition.type().getJDKType()::cast);
  }

  @Override
  public Optional<?> get(String name) {
    return ofNullable(cellsByName.get(name)).map(Cell::value);
  }

  /**
   * Adds the specified {@code Cell} to this set if a cell having the same cell name is not already present.
   * @param cell the new {@code Cell} to add to this set
   * @return if this set did not already contain the specified cell
   * @throws NullPointerException if {@code cell} is {@code null}
   */
  @Override
  public boolean add(Cell<?> cell) {
    return cellsByName.putIfAbsent(cell.definition().name(), cell) == null;
  }

  /**
   * Adds the specified {@code Cell} to this set if a cell having the same name is not already present or
   * replaces the existing cell if a cell of the same name is already present.
   * @param cell the new {@code Cell} to put into this set
   * @return the previous {@code Cell} having the same name as {@code cell}, if any; {@code null} if a like-named
   *        cell is not present
   * @throws NullPointerException if {@code cell} is {@code null}
   */
  public Cell<?> set(Cell<?> cell) {
    return cellsByName.put(cell.definition().name(), cell);
  }

  /**
   * Returns {@code true} if this set contains the specified {@code Cell} or another cell having the same name.
   * @param o {@inheritDoc}
   * @return {@code true} if this set contains the specified cell
   * @throws NullPointerException if {@code o} is {@code null}
   * @throws ClassCastException if {@code o} is not a {@link Cell}
   */
  @Override
  public boolean contains(Object o) {
    Cell<?> c = (Cell<?>) o;
    return c.equals(cellsByName.get(c.definition().name())) || cellsByName.containsValue(o);
  }

  /**
   * Removes the specified {@code Cell}.
   * @param o {@inheritDoc}
   * @return {@code true} if this set contained the specified cell
   * @throws NullPointerException if {@code o} is {@code null}
   * @throws ClassCastException if {@code o} is not a {@link Cell}
   */
  @Override
  public boolean remove(Object o) {
    Cell<?> c = (Cell<?>) o;
    return cellsByName.remove(c.definition().name(), c);
  }

  /**
   * Removes a {@code Cell} with the specified name.
   * @param name name of cell to be removed
   * @return an optional containing the removed cell
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public Optional<Cell<?>> remove(String name) {
    return ofNullable(cellsByName.remove(requireNonNull(name)));
  }

  /**
   * Removes the {@code Cell} with this definition from this set if it is present.
   * @param <T> the cell JDK type
   * @param definition cell definition to remove
   * @return an optional containing the removed cell
   * @throws NullPointerException if {@code definition} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<Cell<T>> remove(CellDefinition<T> definition) {
    while (true) {
      Cell<?> c = cellsByName.get(definition.name());

      if (c != null && definition.equals(c.definition())) {
        if (cellsByName.remove(c.definition().name(), c)) {
          return Optional.of((Cell<T>) c);
        }
      } else {
        return empty();
      }
    }
  }

  /**
   * Creates a {@link Spliterator} over the cells in this set.  This {@code Spliterator} reports
   * {@link Spliterator#DISTINCT}, {@link Spliterator#NONNULL}, and {@link Spliterator#SIZED}.
   * @return a {@code Spliterator} over the cells in this set
   */
  @Override
  public Spliterator<Cell<?>> spliterator() {
    return new DelegatingSpliterator<>(cellsByName.values().spliterator(), Spliterator.DISTINCT | Spliterator.SIZED);
  }

  /**
   * Returns an {@link Iterator} over the cells in this set.  The cells are returned in no particular order.
   * @return an {@code Iterator} over the cells in this set
   */
  @Override
  public Iterator<Cell<?>> iterator() {
    return cellsByName.values().iterator();
  }

  /**
   * {@inheritDoc}
   * @return {@inheritDoc}
   */
  @Override
  public int size() {
    return cellsByName.size();
  }

  private static final class DelegatingSpliterator<E> implements Spliterator<E> {
    private final Spliterator<E> delegate;
    private final int additionalCharacteristics;

    private DelegatingSpliterator(Spliterator<E> delegate, int additionalCharacteristics) {
      this.delegate = delegate;
      this.additionalCharacteristics = additionalCharacteristics;
    }

    @Override
    public boolean tryAdvance(Consumer<? super E> action) {
      return delegate.tryAdvance(action);
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
      delegate.forEachRemaining(action);
    }

    @Override
    public Spliterator<E> trySplit() {
      Spliterator<E> split = delegate.trySplit();
      return (split == null ? null : new DelegatingSpliterator<>(split, additionalCharacteristics));
    }

    @Override
    public long estimateSize() {
      return delegate.estimateSize();
    }

    @Override
    public long getExactSizeIfKnown() {
      return ((characteristics() & Spliterator.SIZED) == 0 ? -1L : estimateSize());
    }

    @Override
    public int characteristics() {
      return delegate.characteristics() | additionalCharacteristics;
    }

    @Override
    public boolean hasCharacteristics(int characteristics) {
      return (characteristics() & characteristics) == characteristics;
    }

    @Override
    public Comparator<? super E> getComparator() {
      return delegate.getComparator();
    }
  }
}
