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

package com.terracottatech.sovereign.common.valuepile;

import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 10/3/2016.
 */
public class ValueStackWriterImplTest {

  @Test
  public void testSimple() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneByte(1);
    rw.oneByte(1);
    rw.oneBoolean(true).oneBoolean(false);
    rw.oneFloat(1.1f);
    rw.oneInt(Integer.MAX_VALUE);
    rw.oneInt(Integer.MIN_VALUE);
    rw.oneLong(Long.MAX_VALUE);
    rw.oneLong(Long.MIN_VALUE);
    rw.bytes(new byte[0], 0, 0);
    rw.utfString("");
    rw.utfString("wubbzy!!!!!");
    int wrote = rw.finish();
    assertThat(rw.getFieldCount(), is(12));
    assertThat(wrote, is(65));
  }
}