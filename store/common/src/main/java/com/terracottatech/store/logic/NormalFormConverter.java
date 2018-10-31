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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.NExpression;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.Variable;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts an {@link Expression} in a normal form into a {@link NormalForm} object.
 */
class NormalFormConverter<R> {

  static final String UNKNOWN_NF_TYPE = "Unknown normal form type.";
  private static final String NOT_IN_NF = "Input expression is not in a normal form.";

  NormalForm<R, IntrinsicPredicate<R>> createNormalForm(Expression<IntrinsicWrapper<R>> normalized, NormalForm.Type type) {
    if (normalized instanceof Variable) {
      return createNormalForm((Variable<IntrinsicWrapper<R>>) normalized, type);
    } else if (normalized instanceof Not) {
      return createNormalForm((Not<IntrinsicWrapper<R>>) normalized, type);
    } else if (normalized instanceof And) {
      return createNormalForm((And<IntrinsicWrapper<R>>) normalized, type);
    } else if (normalized instanceof Or) {
      return createNormalForm((Or<IntrinsicWrapper<R>>) normalized, type);
    } else if (normalized instanceof Literal) {
      boolean value = ((Literal) normalized).getValue();
      return NormalForm.constant(type, value);
    } else {
      throw new AssertionError(NOT_IN_NF);
    }
  }

  private NormalForm<R, IntrinsicPredicate<R>> createNormalForm(Variable<IntrinsicWrapper<R>> variable, NormalForm.Type type) {
    IntrinsicPredicate<R> atom = variable.getValue().
            getIntrinsic();
    return createNormalForm(type, atom);
  }

  private NormalForm<R, IntrinsicPredicate<R>> createNormalForm(NormalForm.Type type, IntrinsicPredicate<R> atom) {
    return NormalForm.<R, IntrinsicPredicate<R>>builder(type)
            .addClause(atom)
            .build();
  }

  private NormalForm<R, IntrinsicPredicate<R>> createNormalForm(Not<IntrinsicWrapper<R>> not, NormalForm.Type type) {
    IntrinsicPredicate<R> atom = getNegatedValue(not);
    return createNormalForm(type, atom);
  }

  private IntrinsicPredicate<R> getNegatedValue(Not<IntrinsicWrapper<R>> not) {
    Expression<IntrinsicWrapper<R>> expression = not.getE();
    if (expression instanceof Variable) {
      Variable<IntrinsicWrapper<R>> variable = (Variable<IntrinsicWrapper<R>>) expression;
      return variable.getValue()
              .getIntrinsic()
              .negate();
    } else {
      throw new AssertionError(NOT_IN_NF);
    }
  }

  private NormalForm<R, IntrinsicPredicate<R>> createNormalForm(And<IntrinsicWrapper<R>> and, NormalForm.Type type) {
    switch (type) {
      case CONJUNCTIVE:
        return processTopOperator(and, type);
      case DISJUNCTIVE:
        return processBottomOperator(type, createLiteralsForClause(and));
      default:
        throw new AssertionError(UNKNOWN_NF_TYPE);
    }
  }

  private NormalForm<R, IntrinsicPredicate<R>> createNormalForm(Or<IntrinsicWrapper<R>> or, NormalForm.Type type) {
    switch (type) {
      case DISJUNCTIVE:
        return processTopOperator(or, type);
      case CONJUNCTIVE:
        return processBottomOperator(type, createLiteralsForClause(or));
      default:
        throw new AssertionError(UNKNOWN_NF_TYPE);
    }
  }

  private NormalForm<R, IntrinsicPredicate<R>> processTopOperator(NExpression<IntrinsicWrapper<R>> operator, NormalForm.Type type) {
    NormalForm.Builder<R, IntrinsicPredicate<R>> builder = NormalForm.builder(type);
    operator.getChildren()
            .stream()
            .map(this::createLiteralsForClause)
            .forEach(builder::addClause);
    return builder.build();
  }

  private Set<IntrinsicPredicate<R>> createLiteralsForClause(Expression<IntrinsicWrapper<R>> child) {
    if (child instanceof Variable) {
      IntrinsicPredicate<R> literal = createLiteral((Variable<IntrinsicWrapper<R>>) child);
      return Collections.singleton(literal);
    } else if (child instanceof Not) {
      IntrinsicPredicate<R> literal = createNegatedLiteral((Not<IntrinsicWrapper<R>>) child);
      return Collections.singleton(literal);
    } else if (child instanceof NExpression) {
      return createLiteralsForClause((NExpression<IntrinsicWrapper<R>>) child);
    } else {
      throw new AssertionError(NOT_IN_NF);
    }
  }

  private Set<IntrinsicPredicate<R>> createLiteralsForClause(NExpression<IntrinsicWrapper<R>> expression) {
    return expression.getChildren()
            .stream()
            .map(this::createLiteral)
            .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private IntrinsicPredicate<R> createLiteral(Expression<IntrinsicWrapper<R>> child) {
    if (child instanceof Variable) {
      return createLiteral((Variable<IntrinsicWrapper<R>>) child);
    } else if (child instanceof Not) {
      Not<IntrinsicWrapper<R>> not = (Not<IntrinsicWrapper<R>>) child;
      return createNegatedLiteral(not);
    } else {
      throw new AssertionError(NOT_IN_NF);
    }
  }

  private IntrinsicPredicate<R> createLiteral(Variable<IntrinsicWrapper<R>> child) {
    return child.getValue().getIntrinsic();
  }

  private IntrinsicPredicate<R> createNegatedLiteral(Not<IntrinsicWrapper<R>> not) {
    return getNegatedValue(not);
  }

  private NormalForm<R, IntrinsicPredicate<R>> processBottomOperator(NormalForm.Type type, Set<IntrinsicPredicate<R>> children) {
    return NormalForm.<R, IntrinsicPredicate<R>>builder(type)
            .addClause(children)
            .build();
  }
}
