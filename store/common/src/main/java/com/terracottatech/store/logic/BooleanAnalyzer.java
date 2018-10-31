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

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;

/**
 * Convert intrinsic predicates to NormalForms.
 */
public class BooleanAnalyzer<R> {

  public NormalForm<R, IntrinsicPredicate<R>> toNormalForm(IntrinsicPredicate<R> predicate, NormalForm.Type type) {
    Expression<IntrinsicWrapper<R>> expression = new BooleanExpressionBuilder<>(predicate)
            .build();
    Expression<IntrinsicWrapper<R>> normalized = toNormalForm(expression, type);
    return new NormalFormConverter<R>().createNormalForm(normalized, type);
  }

  private Expression<IntrinsicWrapper<R>> toNormalForm(Expression<IntrinsicWrapper<R>> expression, NormalForm.Type type) {
    switch (type) {
      case CONJUNCTIVE:
        return RuleSet.toCNF(expression);
      case DISJUNCTIVE:
        return RuleSet.toDNF(expression);
      default:
        throw new AssertionError(NormalFormConverter.UNKNOWN_NF_TYPE);
    }
  }
}
