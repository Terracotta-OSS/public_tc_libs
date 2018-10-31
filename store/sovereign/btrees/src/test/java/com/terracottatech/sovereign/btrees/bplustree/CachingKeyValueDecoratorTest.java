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

package com.terracottatech.sovereign.btrees.bplustree;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.LongFunction;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class CachingKeyValueDecoratorTest {

  static final LongFunction<String> keyfunc=new LongFunction<String>()
  {
    @Override
    public String apply(long value) {
      return "K"+value;
    }
  };

  static final LongFunction<String> valfunc=new LongFunction<String>()
  {
    @Override
    public String apply(long value) {
      return "V"+value;
    }
  };

  static final LongFunction<String> nullfunc=new LongFunction<String>()
  {
    @Override
    public String apply(long value) {
      return null;
    }
  };

  @Test
  public void testCreation() {
    CachingKeyValueDecorator<String, String> dec = new CachingKeyValueDecorator<String, String>(100, 100);
  }

  @Test
  public void testCacheMiss() {
    CachingKeyValueDecorator<String, String> dec = new CachingKeyValueDecorator<String, String>(100, 100);
    Assert.assertThat(dec.getKey(1, nullfunc),nullValue());
    Assert.assertThat(dec.getValue(1, nullfunc),nullValue());
  }

  @Test
  public void testCacheGenerate() {
    CachingKeyValueDecorator<String, String> dec = new CachingKeyValueDecorator<String, String>(100, 100);
    Assert.assertThat(dec.getKey(1, keyfunc),is("K1"));
    Assert.assertThat(dec.getValue(1, valfunc),is("V1"));
  }

  @Test
  public void testCacheHit() {
    CachingKeyValueDecorator<String, String> dec = new CachingKeyValueDecorator<String, String>(100, 100);
    dec.getKey(1, keyfunc);
    dec.getValue(1, valfunc);
    Assert.assertThat(dec.getKey(1, nullfunc),is("K1"));
    Assert.assertThat(dec.getValue(1, nullfunc),is("V1"));
  }

}
