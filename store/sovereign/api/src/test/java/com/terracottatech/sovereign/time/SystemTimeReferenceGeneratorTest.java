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

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests {@link com.terracottatech.store.time.SystemTimeReference.Generator} operation.
 *
 * @author Clifford W. Johnson
 */
public class SystemTimeReferenceGeneratorTest {

  @Test
  public void testPutNullBuffer() throws Exception {
    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    final SystemTimeReference timeReference = generator.get();

    try {
      generator.put(null, timeReference);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutNullTimeReference() throws Exception {
    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    try {
      generator.put(buffer, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testGetNullBuffer() throws Exception {
    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    try {
      generator.get(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testPutGet() throws Exception {
    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    final SystemTimeReference originalTimeReference = generator.get();
    final ByteBuffer buffer = ByteBuffer.allocate(4096);

    final int initialPosition = buffer.position();
    generator.put(buffer, originalTimeReference);
    assertThat(buffer.position(), not(equalTo(initialPosition)));

    buffer.flip();

    final SystemTimeReference deserializedTimeReference = generator.get(buffer);
    assertThat(deserializedTimeReference, comparesEqualTo(originalTimeReference));
  }
}
