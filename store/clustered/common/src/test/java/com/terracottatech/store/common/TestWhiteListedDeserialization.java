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

package com.terracottatech.store.common;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import static com.terracottatech.store.common.ByteBufferUtil.deserializeWhiteListed;
import static com.terracottatech.store.common.ByteBufferUtil.serialize;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestWhiteListedDeserialization {

  @Test
  public void nonWhiteListedClassTest() {
    String className = ObjectStreamClass.lookup(Float.class).getName();
    Float i = 10.0f;
    ByteBuffer byteBuffer = ByteBufferUtil.serialize(i);

    Float j;

    try {
      j = ByteBufferUtil.deserializeWhiteListed(byteBuffer, Float.class, Arrays.asList(Integer.class, Double.class));
      fail("Exception was expected to be thrown here");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().equals("Class deserialization of " + className + " blocked by whitelist."));
    }
  }

  @Test
  public void testByteBufferUtil() throws Exception {
    Date date = new Date();

    Date deserialized = deserializeWhiteListed(serialize(date), Date.class, Arrays.asList(Date.class));
    Assert.assertThat(deserialized, Matchers.is(date));
  }
}
