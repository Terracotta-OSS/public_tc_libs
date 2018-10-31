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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link Constant} classes.
 */
public class ConstantTest {

  @Test
  public void testConstant() {
    Object constant = new Constant<>("a");
    assertEquals(constant, constant);
    assertEquals(constant.hashCode(), constant.hashCode());

    Object same = new Constant<>("a");
    assertEquals(constant, same);
    assertEquals(constant.hashCode(), same.hashCode());

    Object other = new Constant<>("b");
    assertNotEquals(constant, other);
  }
}
