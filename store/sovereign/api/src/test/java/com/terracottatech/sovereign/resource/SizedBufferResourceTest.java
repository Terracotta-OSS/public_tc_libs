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
package com.terracottatech.sovereign.resource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SizedBufferResourceTest {
  @Test
  public void reserveAndFree() {
    SizedBufferResource resource = new SizedBufferResource(1000);
    assertEquals(1000, resource.getSize());
    assertEquals(1000, resource.getAvailable());

    assertFalse(resource.reserve(1001));
    assertEquals(1000, resource.getSize());
    assertEquals(1000, resource.getAvailable());

    assertTrue(resource.reserve(1000));
    assertEquals(1000, resource.getSize());
    assertEquals(0, resource.getAvailable());

    assertFalse(resource.reserve(1));
    assertEquals(1000, resource.getSize());
    assertEquals(0, resource.getAvailable());


    resource.free(10);
    assertEquals(1000, resource.getSize());
    assertEquals(10, resource.getAvailable());


    assertFalse(resource.reserve(11));
    assertEquals(1000, resource.getSize());
    assertEquals(10, resource.getAvailable());

    assertTrue(resource.reserve(10));
    assertEquals(1000, resource.getSize());
    assertEquals(0, resource.getAvailable());

    assertFalse(resource.reserve(1));
    assertEquals(1000, resource.getSize());
    assertEquals(0, resource.getAvailable());
  }
}
