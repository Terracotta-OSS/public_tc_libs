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

import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;

public class ShardIteratorTest {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);

  private final SovereignDatasetImpl<Integer> dataset = (SovereignDatasetImpl<Integer>) new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
          .alias("test")
            .concurrency(2)
            .offheap(4096 * 1024)
            .build();

  @Test
  public void testIterateOverShards() {
    Random random = new Random();

    Set<Integer> added = new HashSet<>();
    random.ints(200).forEach(x -> {
      this.dataset.add(IMMEDIATE, x, nameCell.newCell(Integer.toString(x)));
      added.add(x);
    });

    try(ShardIterator iterator = this.dataset.iterate(0)){
      while (iterator.hasNext()) {
        BufferDataTuple next = iterator.next();
        added.remove(getKey(next.getData()));
      }
    } catch (Exception e) {
      Assert.fail();
    }

    try(ShardIterator iterator = this.dataset.iterate(1)){
      while (iterator.hasNext()) {
        BufferDataTuple next = iterator.next();
        added.remove(getKey(next.getData()));
      }
    } catch (Exception e) {
      Assert.fail();
    }

    Assert.assertTrue(added.isEmpty());
  }

  private Integer getKey(ByteBuffer buffer) {
    if(buffer == null) {
      throw new NullPointerException();
    }
    return this.dataset.getRuntime().getBufferStrategy().fromByteBuffer(buffer).getKey();
  }
}
