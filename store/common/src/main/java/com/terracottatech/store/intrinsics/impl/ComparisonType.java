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
package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Type;

/**
 * Enumerates the comparison operator types.
 */
public enum ComparisonType {

  EQ {
    @Override
    public ComparisonType negate() {
      return NEQ;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison == 0;
    }

    @Override
    public <T> boolean evaluate(T left, T right) {
      return Type.equals(left, right);
    }

    @Override
    public String toString() {
      return "==";
    }
  },

  NEQ {
    @Override
    public ComparisonType negate() {
      return EQ;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison != 0;
    }

    @Override
    public <T> boolean evaluate(T left, T right) {
      return !Type.equals(left, right);
    }

    @Override
    public String toString() {
      return "!=";
    }
  },

  GREATER_THAN {
    @Override
    public ComparisonType negate() {
      return LESS_THAN_OR_EQUAL;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison > 0;
    }

    @Override
    public String toString() {
      return ">";
    }
  },

  GREATER_THAN_OR_EQUAL {
    @Override
    public ComparisonType negate() {
      return LESS_THAN;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison >= 0;
    }

    @Override
    public String toString() {
      return ">=";
    }
  },

  LESS_THAN {
    @Override
    public ComparisonType negate() {
      return GREATER_THAN_OR_EQUAL;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison < 0;
    }

    @Override
    public String toString() {
      return "<";
    }
  },

  LESS_THAN_OR_EQUAL {
    @Override
    public ComparisonType negate() {
      return GREATER_THAN;
    }

    @Override
    public boolean evaluate(int comparison) {
      return comparison <= 0;
    }

    @Override
    public String toString() {
      return "<=";
    }
  };

  public abstract ComparisonType negate();

  public abstract boolean evaluate(int comparison);

  // Override for non-Comparable equals comparison
  @SuppressWarnings("unchecked")
  public <T> boolean evaluate(T left, T right) {
    return evaluate((Comparable)left, (Comparable)right);
  }

  public final <T extends Comparable<T>> boolean evaluate(T left, T right) {
    return evaluate(left.compareTo(right));
  }
}
