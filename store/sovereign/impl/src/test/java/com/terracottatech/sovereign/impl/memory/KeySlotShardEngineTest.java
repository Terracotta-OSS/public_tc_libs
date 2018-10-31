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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 2/15/2016.
 */
public class KeySlotShardEngineTest  {

  @Test
  public void testsimple8bits() throws Exception {
    KeySlotShardEngine le = new KeySlotShardEngine(15, 8);
    assertThat(le.getBitPosition(),is(15));
    assertThat(le.getBitsize(),is(8));
    assertThat(le.getBitMask(),is(0xff00l));

    KeySlotShardEngine.LongToLong add1 = le.addShardLambdaFor(1);
    KeySlotShardEngine.LongToInt getShard = le.extractShardIndexLambda();
    KeySlotShardEngine.LongToLong removeShard = le.removeShardIndexLambda();

    long p=2;
    p=add1.transmute(p);
    assertThat(getShard.transmute(p), is(1));
    assertThat(removeShard.transmute(p), is(2l));

  }
}
