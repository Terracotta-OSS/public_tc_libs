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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Finite collection of literals (defined as predicates) that is
 * true either whenever at least one of the literals that form it is true (a disjunctive clause),
 * or when all of the literals that form it are true (a conjunctive clause).
 */
public class Clause<V, P extends Predicate<V>> {

  private final Set<P> literals;
  private final Operator operator;

  Clause(Operator operator, Set<P> literals) {
    this.literals = Collections.unmodifiableSet(literals);
    this.operator = operator;
  }

  public Stream<P> literals() {
    return literals.stream();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Clause<?, ?> clause = (Clause) o;
    return Objects.equals(literals, clause.literals);
  }

  @Override
  public int hashCode() {
    return Objects.hash(literals);
  }

  @Override
  public String toString() {
    String delimiter = " " + operator.getSymbol() + " ";
    return literals()
            .map(String::valueOf)
            .collect(Collectors.joining(delimiter, "(", ")"));
  }
}
