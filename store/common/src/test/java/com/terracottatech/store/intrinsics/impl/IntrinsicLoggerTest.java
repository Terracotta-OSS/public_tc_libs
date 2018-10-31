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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link IntrinsicLogger} classes.
 */
public class IntrinsicLoggerTest {

  @Test
  public void testIntrinsicLogger() {
    Object logger = new IntrinsicLogger<>("a", singletonList(new Constant<>(0.0)));
    assertEquals(logger, logger);
    assertEquals(logger.hashCode(), logger.hashCode());

    Object same = new IntrinsicLogger<>("a", singletonList(new Constant<>(0.0)));
    assertEquals(logger, same);
    assertEquals(logger.hashCode(), same.hashCode());

    Object otherMessage = new IntrinsicLogger<>("b", singletonList(new Constant<>(0.0)));
    assertNotEquals(logger, otherMessage);

    Object otherMappers = new IntrinsicLogger<>("a", singletonList(new Constant<>(1.0)));
    assertNotEquals(logger, otherMappers);
  }
}
