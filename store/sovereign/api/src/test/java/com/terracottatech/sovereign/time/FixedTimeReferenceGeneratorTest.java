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

package com.terracottatech.sovereign.time;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

/**
 * Tests operation of {@link FixedTimeReference.Generator}.
 *
 * @author Clifford W. Johnson
 */
public class FixedTimeReferenceGeneratorTest {

  @Test
  public void testType() throws Exception {
    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    assertThat(generator.type(), equalTo(FixedTimeReference.class));
  }

  @Test
  public void testGet() throws Exception {
    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    assertThat(generator.get(), sameInstance(generator.get()));
  }

  @Test
  public void testMaxSerializedLength() throws Exception {
    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    assertThat(generator.maxSerializedLength(), equalTo(0));
  }

  @Test
  public void testPut() throws Exception {
    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    final FixedTimeReference timeReference = generator.get();
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    generator.put(buffer, timeReference);
    assertThat(buffer.position(), equalTo(0));
  }

  @Test
  public void testGet1() throws Exception {
    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    assertThat(generator.get(buffer), sameInstance(generator.get()));
  }
}
