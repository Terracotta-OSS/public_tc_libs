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

package com.terracottatech.store.logic;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.logic.Operator.CONJUNCTION;
import static com.terracottatech.store.logic.Operator.DISJUNCTION;

/**
 * Conjunctive or disjunctive normal form.
 */
public class NormalForm<V, P extends Predicate<V>> {

  private NormalForm(Type type, Set<Clause<V, P>> clauses) {
    this.type = type;
    this.clauses = Collections.unmodifiableSet(clauses);
  }

  public enum Type {
    CONJUNCTIVE(CONJUNCTION, DISJUNCTION),
    DISJUNCTIVE(DISJUNCTION, CONJUNCTION);

    private final Operator externalOperator;
    private final Operator internalOperator;

    Type(Operator operator, Operator clauseOperator) {
      this.externalOperator = operator;
      this.internalOperator = clauseOperator;
    }

    public String getSymbol() {
      return externalOperator.getSymbol();
    }
  }

  private final Type type;
  private final Set<Clause<V, P>> clauses;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NormalForm<?, ?> that = (NormalForm) o;
    return type == that.type &&
            Objects.equals(clauses, that.clauses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, clauses);
  }

  @Override
  public String toString() {
    return clauses()
            .map(String::valueOf)
            .collect(Collectors.joining(type.getSymbol(), "(", ")"));
  }

  public Stream<Clause<V, P>> clauses() {
    return clauses.stream();
  }

  public Type getType() {
    return type;
  }

  /**
   * NF always evaluates to false.
   *
   * @return true if NF always evaluates to false, else false.
   */
  public boolean isContradiction() {
    return false;
  }

  /**
   * NF always evaluates to true.
   *
   * @return true if NF always evaluates to true, else false.
   */
  public boolean isTautology() {
    return false;
  }

  static <V, P extends Predicate<V>> Builder<V, P> builder(Type type) {
    return new Builder<>(type);
  }

  static class Builder<V, P extends Predicate<V>> {
    private final Type type;
    private Set<Clause<V, P>> clauses = new LinkedHashSet<>();

    Builder(Type type) {
      this.type = type;
    }

    @SuppressWarnings("varargs")
    @SafeVarargs
    final Builder<V, P> addClause(P... literals) {
      addClause(new LinkedHashSet<>(Arrays.asList(literals)));
      return this;
    }

    Builder<V, P> addClause(Set<P> literals) {
      Clause<V, P> clause = new Clause<>(type.internalOperator, literals);
      clauses.add(clause);
      return this;
    }

    NormalForm<V, P> build() {
      return new NormalForm<>(type, clauses);
    }
  }

  static <V, P extends Predicate<V>> NormalForm<V, P> constant(Type type, boolean value) {
    return new Constant<>(type, value);
  }

  private static class Constant<V, P extends Predicate<V>> extends NormalForm<V, P> {

    private final boolean value;

    Constant(Type type, boolean value) {
      super(type, Collections.emptySet());
      this.value = value;
    }

    @Override
    public String toString() {
      return Boolean.toString(value);
    }

    @Override
    public boolean isContradiction() {
      return !value;
    }

    @Override
    public boolean isTautology() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Constant<?, ?> constant = (Constant) o;
      return value == constant.value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), value);
    }
  }
}
