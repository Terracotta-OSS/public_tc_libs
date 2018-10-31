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
package com.terracottatech.sovereign.common.utils;

import java.math.BigInteger;
import java.util.NoSuchElementException;

/**
 * This uses a psuedo rng to generate a fairly random, and complete, sequence of integers.
 *
 * Adapted in bits from this article:
 * http://preshing.com/20121224/how-to-generate-a-sequence-of-unique-random-integers/
 *
 * @author cschanck
 */
public class SequenceGenerator {
  private final int max;
  private final boolean wrap;
  private int prime;
  private BigInteger index = BigInteger.valueOf(0);

  /**
   * Instantiates a new Sequence generator.
   *
   * @param max the max (0 to max-1 will be the range)
   * @param wrap is the generator allowed to wrap?
   */
  public SequenceGenerator(int max, boolean wrap) {
    if (max <= 0) {
      throw new IllegalArgumentException();
    }
    this.wrap = wrap;
    this.max = max;

    this.prime = max;
    if (prime % 4 != 0) {
      prime = (prime / 4 + 1) * 4;
    }
    prime = prime - 1;
    while (!isPrime(prime)) {
      prime = prime + 4;
    }
  }

  public void reset() {
    index = BigInteger.valueOf(0);
  }

  private boolean isPrime(long n) {
    if (n <= 3) {
      return n > 1;
    }
    if (n % 2 == 0 || n % 3 == 0) {
      return false;
    }
    for (int i = 5; i < Math.sqrt(n) + 1; i += 6) {
      if (n % i == 0 || n % (i + 2) == 0) {
        return false;
      }
    }
    return true;
  }

  private int permuteQPR(BigInteger x) {
    BigInteger residue = x.pow(2).mod(BigInteger.valueOf(prime));
    if (x.intValue() <= (prime / 2)) {
      return residue.intValue();
    }
    return prime - residue.intValue();
  }

  public int next() {
    while (true) {
      int ret = permuteQPR(index);
      index = index.add(BigInteger.ONE);
      if (ret < max) {
        return ret;
      }
      if (index.intValue() >= prime) {
        if (wrap) {
          reset();
        } else {
          throw new NoSuchElementException();
        }
      }
    }
  }

  @Override
  public String toString() {
    return "SequenceGenerator{" +
      "max=" + max +
      ", prime=" + prime +
      '}';
  }

}
