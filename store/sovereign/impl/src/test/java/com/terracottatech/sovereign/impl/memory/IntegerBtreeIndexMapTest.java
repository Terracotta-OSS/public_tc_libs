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
package com.terracottatech.sovereign.impl.memory;

import org.junit.Test;

import java.util.Random;
import java.util.function.Supplier;

/**
 *
 * @author mscott
 */
public class IntegerBtreeIndexMapTest extends BtreeIndexMapTest<Integer> {

  public IntegerBtreeIndexMapTest() {
  }

  @Override
  public Class<Integer> keyType() {
    return Integer.class;
  }

  public void testAdd() {
    super.add(new Integer[]{30, 1, 25});
  }

  @Test
  public void testdups() throws Exception {
    super.internalTestDups(new Integer[]{1, 2, 3, 5, 5, 5, -1, -10});
  }

  @Test
  public void testProperDeletion()  throws Exception {
    super.testDeletion(new Integer[]{1, 2, 3, 5, 5, 5, -1, -10});
  }

  @Test
  public void testmiddle() throws Exception {
    super.middlecount(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 44, 44, 44, 44, 44, 44, 44, 44, -1111, -101}, 44);
  }

  @Test
  public void testhigher() throws Exception {
//  test in the middle of dups
    super.higher(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, 5, 4);
//  test past the end
    super.higher(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, 47, 0);
//  test past the beginning
    super.higher(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, Integer.MIN_VALUE, 12);
  }


  @Test
  public void testlower() throws Exception {
    super.lower(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, 44, 8);
//  test past the end
    super.lower(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, Integer.MIN_VALUE, 0);
//  test past the beginning
    super.lower(new Integer[]{1, 2, 3, 5, 5, 5, 44, 44, 45, 46, -1111, -101}, Integer.MAX_VALUE, 12);
  }

  @Test
  public void testBatch() throws Exception {

    Supplier<Integer> sup=new Supplier<Integer>() {
      Random r=new Random();
      @Override
      public Integer get() {
        return r.nextInt();
      }
    };
    testBatchIngestion(100000, sup);
  }
}
