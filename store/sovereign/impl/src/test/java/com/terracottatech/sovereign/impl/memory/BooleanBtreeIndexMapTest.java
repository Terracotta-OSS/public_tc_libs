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
public class BooleanBtreeIndexMapTest extends BtreeIndexMapTest<Boolean> {
  // TODO XXX
  public BooleanBtreeIndexMapTest() {
  }

  @Override
  public Class<Boolean> keyType() {
    return Boolean.class;
  }

  public void testAdd() {
    super.add(new Boolean[]{false, false, true});
  }

  @Test
  public void testdups() throws Exception {
    super.internalTestDups(new Boolean[]{false, false , false, true, true, true, false, true, false});
  }

  @Test
  public void testProperDeletion()  throws Exception {
    super.testDeletion(new Boolean[]{false, false , false, true, true, true, false, true, false});
  }

  @Test
  public void testmiddle() throws Exception {
    super.middlecount(new Boolean[]{false, false , false, true, true, true, false, true, false, true, true, false, true, true, true, true}, true);
  }

  @Test
  public void testBatch() throws Exception {

    Supplier<Boolean> sup=new Supplier<Boolean>() {
      Random r=new Random();
      @Override
      public Boolean get() {
        return r.nextBoolean();
      }
    };
    testBatchIngestion(100000, sup);
  }
}
