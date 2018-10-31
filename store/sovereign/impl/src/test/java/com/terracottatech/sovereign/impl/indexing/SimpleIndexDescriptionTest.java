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

package com.terracottatech.sovereign.impl.indexing;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 4/30/2015.
 */
public class SimpleIndexDescriptionTest {

  @Test
  public void testCreation() {
    SimpleIndexDescription<Integer> descr = new SimpleIndexDescription<>(SovereignIndex.State.LIVE, "foo",
        Type.INT, SovereignIndexSettings.BTREE);
    assertThat(descr.getCellDefinition(), is(CellDefinition.define("foo", Type.INT)));
    assertThat(descr.getCellName(), is("foo"));
    assertThat(descr.getCellType(), is(Type.INT));
    assertThat(descr.getIndexSettings(), is(SovereignIndexSettings.BTREE));
  }

  @Test
  public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
    SimpleIndexDescription<Integer> descr = new SimpleIndexDescription<>(SovereignIndex.State.LIVE, "foo",
        Type.INT, SovereignIndexSettings.BTREE);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    oos.writeObject(descr);
    oos.flush();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);

    @SuppressWarnings("unchecked")
    SimpleIndexDescription<Integer> descr2 = (SimpleIndexDescription<Integer>) ois.readObject();

    assertThat(descr2.getCellDefinition(), is(CellDefinition.define("foo", Type.INT)));
    assertThat(descr2.getCellName(), is("foo"));
    assertThat(descr2.getCellType(), is(Type.INT));
    assertThat(descr2.getIndexSettings().isSorted(), is(SovereignIndexSettings.BTREE.isSorted()));
    assertThat(descr2.getIndexSettings().isUnique(), is(SovereignIndexSettings.BTREE.isUnique()));
  }

  @Test
  public void testGetDescriptionFromIndex() throws Exception {
    SovereignDataset<Integer> sb = new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    try {
      CellDefinition<Integer> def1 = CellDefinition.define("foo", Type.INT);
      SovereignIndex<Integer> index = sb.getIndexing().createIndex(def1, SovereignIndexSettings.BTREE).call();

      SovereignIndexDescription<?> descr = index.getDescription();

      assertThat(descr.getCellDefinition(), is(CellDefinition.define("foo", Type.INT)));
      assertThat(descr.getCellName(), is("foo"));
      assertThat(descr.getCellType(), is(Type.INT));
      assertThat(descr.getIndexSettings().isSorted(), is(SovereignIndexSettings.BTREE.isSorted()));
      assertThat(descr.getIndexSettings().isUnique(), is(SovereignIndexSettings.BTREE.isUnique()));
    } finally {
      sb.getStorage().destroyDataSet(sb.getUUID());
    }
  }

  @Test
  public void testMakeIndexFromDescription() throws ExecutionException, InterruptedException, IOException {
    SovereignDataset<Integer> sb = new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    try {
      SimpleIndexDescription<Integer> descr = new SimpleIndexDescription<>(SovereignIndex.State.LIVE, "foo",
          Type.INT, SovereignIndexSettings.BTREE);
      SovereignIndex<Integer> index = new SimpleIndex<>(descr);

      assertThat(index.on(), is(CellDefinition.define("foo", Type.INT)));
      assertThat(index.definition().isSorted(), is(SovereignIndexSettings.BTREE.isSorted()));
      assertThat(index.definition().isUnique(), is(SovereignIndexSettings.BTREE.isUnique()));
    } finally {
      sb.getStorage().destroyDataSet(sb.getUUID());
    }
  }
}
