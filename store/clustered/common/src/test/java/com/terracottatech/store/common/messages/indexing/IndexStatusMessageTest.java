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
package com.terracottatech.store.common.messages.indexing;

import com.terracottatech.store.common.messages.BaseMessageTest;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link IndexStatusMessage}.
 */
public class IndexStatusMessageTest extends BaseMessageTest {

  @Override
  public void testEncodeDecode() throws Exception {
    CellDefinition<String> indexCell = CellDefinition.defineString("stringCell");
    IndexStatusMessage<String> createMessage = new IndexStatusMessage<>(indexCell, IndexSettings.BTREE);

    IndexStatusMessage<?> decodedCreateMessage = encodeDecode(createMessage);
    assertThat(decodedCreateMessage.getCellDefinition(), is(indexCell));
    assertThat(decodedCreateMessage.getIndexSettings(), is(IndexSettings.BTREE));

  }
}