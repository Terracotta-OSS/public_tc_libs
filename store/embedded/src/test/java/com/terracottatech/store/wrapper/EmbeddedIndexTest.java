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
package com.terracottatech.store.wrapper;

import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.junit.Test;

import static com.terracottatech.store.indexing.Index.Status.DEAD;
import static com.terracottatech.store.indexing.Index.Status.INITALIZING;
import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
public class EmbeddedIndexTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testOnDelegatesCorrectly() {
    CellDefinition<Integer> defn = mock(CellDefinition.class);
    SimpleIndex<Integer, ?> delegate = mock(SimpleIndex.class);
    when(delegate.on()).thenReturn(defn);

    EmbeddedIndex<Integer> index = new EmbeddedIndex<>(delegate, null);

    assertThat(index.on(), is(defn));
  }

  @Test
  public void testDefinitionDelegatesCorrectly() {
    IndexSettings settings = mock(IndexSettings.class);
    SovereignIndexSettings sovereignSettings = mock(SovereignIndexSettings.class);
    SimpleIndex<?, ?> delegate = mock(SimpleIndex.class);
    when(delegate.definition()).thenReturn(sovereignSettings);

    EmbeddedIndex<?> index = new EmbeddedIndex<>(delegate, ss -> {
      if (ss == sovereignSettings) {
        return settings;
      } else {
        throw new AssertionError();
      }
    });

    assertThat(index.definition(), is(settings));
  }

  @Test
  public void testGetSovereignIndexReturnsCorrectly() {
    SimpleIndex<?, ?> delegate = mock(SimpleIndex.class);
    EmbeddedIndex<?> index = new EmbeddedIndex<>(delegate, null);

    assertThat(index.getSovereignIndex(), is(delegate));
  }

  @Test
  public void testStatusMapsCorrectly() {
    SimpleIndex<?, ?> delegate = mock(SimpleIndex.class);
    when(delegate.getState()).thenReturn(SovereignIndex.State.UNKNOWN, SovereignIndex.State.CREATED,
      SovereignIndex.State.LOADING,
      SovereignIndex.State.LIVE);
    EmbeddedIndex<?> index = new EmbeddedIndex<>(delegate, null);

    assertThat(index.status(), is(DEAD));
    assertThat(index.status(), is(INITALIZING));
    assertThat(index.status(), is(INITALIZING));
    assertThat(index.status(), is(LIVE));
  }
}
