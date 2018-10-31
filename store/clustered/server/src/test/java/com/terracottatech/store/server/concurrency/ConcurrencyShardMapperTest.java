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
package com.terracottatech.store.server.concurrency;

import org.junit.Test;

import com.terracottatech.store.Type;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ConcurrencyShardMapperTest {

  @Test
  public void getShardCount_TypeBoolean() throws Exception {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.BOOL, Optional.empty());
    assertThat(concurrencyShardMapper.getShardCount(), is(1));
  }

  @Test
  public void getShardCount_TypeLong() throws Exception {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.LONG, Optional.of(2));
    assertThat(concurrencyShardMapper.getShardCount(), is(2));
  }

  @Test
  public void getConcurrencyKey_TypeBoolean() throws Exception {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.BOOL, Optional.empty());
    assertThat(concurrencyShardMapper.getConcurrencyKey(0), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(5), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1234), is(1));

    concurrencyShardMapper = new ConcurrencyShardMapper(Type.BOOL, Optional.of(2));
    assertThat(concurrencyShardMapper.getConcurrencyKey(0), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(5), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1234), is(1));

    concurrencyShardMapper = new ConcurrencyShardMapper(Type.BOOL, Optional.of(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(0), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(5), is(1));
    assertThat(concurrencyShardMapper.getConcurrencyKey(1234), is(1));
  }

  @Test
  public void shardIndexToConcurrencyKey() throws Exception {
    assertThat(ConcurrencyShardMapper.shardIndexToConcurrencyKey(0), is(1));
    assertThat(ConcurrencyShardMapper.shardIndexToConcurrencyKey(1), is(2));
    assertThat(ConcurrencyShardMapper.shardIndexToConcurrencyKey(2), is(3));
    assertThat(ConcurrencyShardMapper.shardIndexToConcurrencyKey(3), is(4));
    assertThat(ConcurrencyShardMapper.shardIndexToConcurrencyKey(4), is(5));
  }

  @Test
  public void concurrencyKeyToShardIndex() throws Exception {
    assertThat(ConcurrencyShardMapper.concurrencyKeyToShardIndex(1), is(0));
    assertThat(ConcurrencyShardMapper.concurrencyKeyToShardIndex(2), is(1));
    assertThat(ConcurrencyShardMapper.concurrencyKeyToShardIndex(3), is(2));
    assertThat(ConcurrencyShardMapper.concurrencyKeyToShardIndex(4), is(3));
  }
}