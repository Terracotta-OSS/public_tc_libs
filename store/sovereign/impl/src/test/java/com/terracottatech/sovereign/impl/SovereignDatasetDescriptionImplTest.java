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

package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

/**
 * Created by cschanck on 4/30/2015.
 */
public class SovereignDatasetDescriptionImplTest {

  @Test
  public void testGetDescriptionWithIndexDefaultAlias() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    try {
      ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();

      SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;
      SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();
      Assert.assertThat(descr.getConfig().getConcurrency(), is(impl.getConfig().getConcurrency()));
      Assert.assertThat(descr.getConfig().getResourceSize(), is(impl.getConfig().getResourceSize()));
      Assert.assertThat(descr.getConfig().getStorageType(), is(impl.getConfig().getStorageType()));
      Assert.assertThat(descr.getConfig().getVersionLimitStrategy(), is(impl.getConfig().getVersionLimitStrategy()));
      Assert.assertThat(descr.getConfig().getTimeReferenceGenerator(), is(impl.getConfig().getTimeReferenceGenerator()));
      Assert.assertThat(descr.getConfig().getRecordLockTimeout(), is(impl.getConfig().getRecordLockTimeout()));
      Assert.assertThat(descr.getConfig().getType(), is(impl.getConfig().getType()));
      Assert.assertThat(descr.getConfig().getVersionLimit(), is(impl.getConfig().getVersionLimit()));
      Assert.assertThat(descr.getUUID(),is(impl.getUUID()));
      Assert.assertThat(descr.getAlias(),is(impl.getUUID().toString()));
      Assert.assertThat(descr.getIndexDescriptions().size(),is(1));
      SimpleIndexDescription<?> ii = descr.getIndexDescriptions().get(0);
      Assert.assertThat(ii.getCellType(),is(Type.STRING));
      Assert.assertThat(ii.getState(),is(SovereignIndex.State.LIVE));
      Assert.assertThat(ii.getCellName(),is("foo"));
      Assert.assertThat(ii.getIndexSettings().isSorted(),is(true));
      Assert.assertThat(ii.getIndexSettings().isUnique(),is(false));
    } finally {
      ds.getStorage().destroyDataSet(ds.getUUID());
    }
  }

  @Test
  public void testGetDescriptionWithIndexWithAlias() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().alias("foobar").build();
    try {
      ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();

      SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;
      SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();
      Assert.assertThat(descr.getConfig().getConcurrency(), is(impl.getConfig().getConcurrency()));
      Assert.assertThat(descr.getConfig().getResourceSize(), is(impl.getConfig().getResourceSize()));
      Assert.assertThat(descr.getConfig().getStorageType(), is(impl.getConfig().getStorageType()));
      Assert.assertThat(descr.getConfig().getVersionLimitStrategy(), is(impl.getConfig().getVersionLimitStrategy()));
      Assert.assertThat(descr.getConfig().getTimeReferenceGenerator(), is(impl.getConfig().getTimeReferenceGenerator()));
      Assert.assertThat(descr.getConfig().getRecordLockTimeout(), is(impl.getConfig().getRecordLockTimeout()));
      Assert.assertThat(descr.getConfig().getType(), is(impl.getConfig().getType()));
      Assert.assertThat(descr.getConfig().getVersionLimit(), is(impl.getConfig().getVersionLimit()));
      Assert.assertThat(descr.getUUID(),is(impl.getUUID()));
      Assert.assertThat(descr.getAlias(),is("foobar"));
      Assert.assertThat(descr.getIndexDescriptions().size(),is(1));
      SimpleIndexDescription<?> ii = descr.getIndexDescriptions().get(0);
      Assert.assertThat(ii.getCellType(),is(Type.STRING));
      Assert.assertThat(ii.getState(),is(SovereignIndex.State.LIVE));
      Assert.assertThat(ii.getCellName(),is("foo"));
      Assert.assertThat(ii.getIndexSettings().isSorted(),is(true));
      Assert.assertThat(ii.getIndexSettings().isUnique(),is(false));
    } finally {
      ds.getStorage().destroyDataSet(ds.getUUID());
    }
  }

  @Test
  public void testRecreateWithDescriptionDefaultAlias() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();
    SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;

    SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();

    SovereignDatasetImpl<Integer> impl2 = new SovereignDatasetImpl<>(descr);

    Assert.assertThat(impl2.getConfig().getConcurrency(), is(impl.getConfig().getConcurrency()));
    Assert.assertThat(impl2.getConfig().getResourceSize(), is(impl.getConfig().getResourceSize()));
    Assert.assertThat(impl2.getConfig().getStorageType(), is(impl.getConfig().getStorageType()));
    Assert.assertThat(impl2.getConfig().getVersionLimitStrategy(), is(impl.getConfig().getVersionLimitStrategy()));
    Assert.assertThat(impl2.getConfig().getTimeReferenceGenerator(), is(impl.getConfig().getTimeReferenceGenerator()));
    Assert.assertThat(impl2.getConfig().getRecordLockTimeout(), is(impl.getConfig().getRecordLockTimeout()));
    Assert.assertThat(impl2.getConfig().getType(), is(impl.getConfig().getType()));
    Assert.assertThat(impl2.getConfig().getVersionLimit(), is(impl.getConfig().getVersionLimit()));
    Assert.assertThat(impl2.getUUID(), is(impl.getUUID()));
    Assert.assertThat(impl2.getAlias(), is(impl.getUUID().toString()));
    Assert.assertThat(impl2.getIndexing().getIndexes().size(), is(0));

    for(SimpleIndexDescription<?> id : descr.getIndexDescriptions()) {
      impl2.getIndexing().createIndex(id.getCellDefinition(), id.getIndexSettings()).call();
    }

    Assert.assertThat(impl2.getIndexing().getIndexes().size(), is(1));
    Assert.assertThat(descr.getIndexDescriptions().size(),is(1));
    SimpleIndexDescription<?> ii = descr.getIndexDescriptions().get(0);
    Assert.assertThat(ii.getCellType(),is(Type.STRING));
    Assert.assertThat(ii.getState(),is(SovereignIndex.State.LIVE));
    Assert.assertThat(ii.getCellName(),is("foo"));
    Assert.assertThat(ii.getIndexSettings().isSorted(),is(true));
    Assert.assertThat(ii.getIndexSettings().isUnique(),is(false));
    impl.dispose();
    impl2.dispose();
  }


  @Test
  public void testRecreateWithDescriptionWithAlias() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).alias("foobar").heap().build();
    ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();
    SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;

    SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();

    SovereignDatasetImpl<Integer> impl2 = new SovereignDatasetImpl<>(descr);

    Assert.assertThat(impl2.getConfig().getConcurrency(), is(impl.getConfig().getConcurrency()));
    Assert.assertThat(impl2.getConfig().getResourceSize(), is(impl.getConfig().getResourceSize()));
    Assert.assertThat(impl2.getConfig().getStorageType(), is(impl.getConfig().getStorageType()));
    Assert.assertThat(impl2.getConfig().getVersionLimitStrategy(), is(impl.getConfig().getVersionLimitStrategy()));
    Assert.assertThat(impl2.getConfig().getTimeReferenceGenerator(), is(impl.getConfig().getTimeReferenceGenerator()));
    Assert.assertThat(impl2.getConfig().getRecordLockTimeout(), is(impl.getConfig().getRecordLockTimeout()));
    Assert.assertThat(impl2.getConfig().getType(), is(impl.getConfig().getType()));
    Assert.assertThat(impl2.getConfig().getVersionLimit(), is(impl.getConfig().getVersionLimit()));
    Assert.assertThat(impl2.getUUID(), is(impl.getUUID()));
    Assert.assertThat(impl2.getAlias(), is("foobar"));
    Assert.assertThat(impl2.getIndexing().getIndexes().size(), is(0));

    for(SimpleIndexDescription<?> id : descr.getIndexDescriptions()) {
      impl2.getIndexing().createIndex(id.getCellDefinition(), id.getIndexSettings()).call();
    }

    Assert.assertThat(impl2.getIndexing().getIndexes().size(), is(1));
    Assert.assertThat(descr.getIndexDescriptions().size(),is(1));
    SimpleIndexDescription<?> ii = descr.getIndexDescriptions().get(0);
    Assert.assertThat(ii.getCellType(),is(Type.STRING));
    Assert.assertThat(ii.getState(),is(SovereignIndex.State.LIVE));
    Assert.assertThat(ii.getCellName(),is("foo"));
    Assert.assertThat(ii.getIndexSettings().isSorted(),is(true));
    Assert.assertThat(ii.getIndexSettings().isUnique(),is(false));
    impl.dispose();
    impl2.dispose();
  }

  @Test
  public void deserializeOriginalVersion() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).alias("alias").offheapResourceName("offheap").build();
    SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;

    SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();
    String uuid = descr.getUUID().toString();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream( baos );
    oos.writeObject( descr );
    oos.close();

    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInput objectInput = new ObjectInputStream(bais);
    SovereignDatasetDescriptionImpl<?, ?> description = (SovereignDatasetDescriptionImpl<?, ?>) objectInput.readObject();

    assertEquals("alias", description.getAlias());
    assertEquals(Optional.of("offheap"), description.getOffheapResourceName());
    assertEquals(uuid, description.getUUID().toString());
  }
}
