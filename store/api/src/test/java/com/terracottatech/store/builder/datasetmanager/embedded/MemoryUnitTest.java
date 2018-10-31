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
package com.terracottatech.store.builder.datasetmanager.embedded;

import com.terracottatech.store.configuration.MemoryUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemoryUnitTest {
  @Test
  public void identity() {
    assertEquals(100L, MemoryUnit.B.toBytes(100));
  }

  @Test
  public void multiply() {
    assertEquals(102400L, MemoryUnit.KB.toBytes(100));
    assertEquals(104857600L, MemoryUnit.MB.toBytes(100));
    assertEquals(107374182400L, MemoryUnit.GB.toBytes(100));
    assertEquals(109951162777600L, MemoryUnit.TB.toBytes(100));
    assertEquals(112589990684262400L, MemoryUnit.PB.toBytes(100));
  }

  @Test(expected = IllegalArgumentException.class)
  public void overflow() {
    MemoryUnit.PB.toBytes(10000);
  }

  @Test
  public void negativeIdentity() {
    assertEquals(-100, MemoryUnit.B.toBytes(-100));
  }

  @Test
  public void negativeMultiply() {
    assertEquals(-102400, MemoryUnit.KB.toBytes(-100));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeOverflow() {
    MemoryUnit.PB.toBytes(-10000);
  }

  @Test
  public void zero() {
    assertEquals(0L, MemoryUnit.B.toBytes(0));
    assertEquals(0L, MemoryUnit.KB.toBytes(0));
    assertEquals(0L, MemoryUnit.MB.toBytes(0));
    assertEquals(0L, MemoryUnit.GB.toBytes(0));
    assertEquals(0L, MemoryUnit.TB.toBytes(0));
    assertEquals(0L, MemoryUnit.PB.toBytes(0));
  }
}
