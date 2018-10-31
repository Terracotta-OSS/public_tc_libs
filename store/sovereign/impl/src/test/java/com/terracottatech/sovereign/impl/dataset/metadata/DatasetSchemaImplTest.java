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

import static com.terracottatech.store.definition.CellDefinition.define;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 8/22/2016.
 */
public class DatasetSchemaImplTest {
  @Test
  public void testSimple() throws Exception {

    DatasetSchemaImpl impl = new DatasetSchemaImpl();

    for (int i = 0; i < 100; i++) {
      int p = i % 5;
      CellDefinition<String> def = define("foo" + p, Type.STRING);
      SchemaCellDefinition<?> got = impl.idFor(def);
      assertThat(got, notNullValue());
    }
    assertThat(impl.definitions().count(), is(5l));
  }

}