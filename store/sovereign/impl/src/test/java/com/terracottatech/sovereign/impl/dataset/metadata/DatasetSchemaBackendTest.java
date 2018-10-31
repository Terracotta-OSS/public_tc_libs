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

package com.terracottatech.sovereign.impl.dataset.metadata;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.terracottatech.store.definition.CellDefinition.define;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 8/18/2016.
 */
public class DatasetSchemaBackendTest {

  @Test
  public void testCreate() throws Exception {
    DatasetSchemaBackend backend = new DatasetSchemaBackend();
    assertThat(backend.definitions()
                        .count(), is(0l));
  }

  @Test
  public void testSet() throws Exception {
    DatasetSchemaBackend backend = new DatasetSchemaBackend();
    for (int i = 0; i < 100; i++) {
      int p = i % 5;
      CellDefinition<String> def = define("foo" + p, Type.STRING);
      SchemaCellDefinition<?> got = backend.idFor(def);
      assertThat(got, notNullValue());
      assertThat(got.definition(), is(def));
    }
    assertThat(backend.definitions().count(), is(5l));
  }


  @Test
  public void testSerDeser() throws IOException, ClassNotFoundException {
    DatasetSchemaBackend backend = new DatasetSchemaBackend();
    for (int i = 0; i < 100; i++) {
      int p = i % 5;
      CellDefinition<String> def = define("foo" + p, Type.STRING);
      SchemaCellDefinition<?> got = backend.idFor(def);
      assertThat(got, notNullValue());
      assertThat(got.definition(), is(def));
    }

    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    oos.writeUnshared(new PersistableSchemaList(backend));
    oos.flush();
    ByteArrayInputStream bais=new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    DatasetSchemaBackend back2 = new DatasetSchemaBackend();
    PersistableSchemaList list= (PersistableSchemaList) ois.readObject();
    back2.setTo(list);
    assertThat(back2.definitions().count(), is(5l));
    assertThat(back2.getCurrentLastMaxId(), greaterThan(Integer.MIN_VALUE));

  }
}