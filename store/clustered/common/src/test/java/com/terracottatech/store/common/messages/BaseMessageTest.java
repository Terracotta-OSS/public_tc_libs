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
package com.terracottatech.store.common.messages;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.Test;
import org.terracotta.entity.MessageCodecException;

import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.common.messages.intrinsics.TestIntrinsicDescriptors;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Provides base for testing {@link DatasetEntityMessage} and {@link DatasetEntityResponse} instances.
 */
public abstract class BaseMessageTest {

  protected final UUID STREAM_ID = UUID.randomUUID();

  private final IntrinsicCodec intrinsicCodec = new IntrinsicCodec(TestIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS);
  private final DatasetOperationMessageCodec datasetMessageCodec = new DatasetOperationMessageCodec(intrinsicCodec);


  /**
   * Required test to test the encode/decode cycle.
   */
  @Test
  public abstract void testEncodeDecode() throws Exception;

  @SuppressWarnings("unchecked")
  protected <M extends DatasetEntityMessage> M encodeDecode(M message) throws MessageCodecException {
    DatasetOperationMessage originalMessage = (DatasetOperationMessage) message;
    byte[] bytes = datasetMessageCodec.encodeMessage(message);
    DatasetOperationMessage datasetOperationMessage = (DatasetOperationMessage) datasetMessageCodec.decodeMessage(bytes);
    assertThat(datasetOperationMessage.getType(), is(originalMessage.getType()));
    assertThat(datasetOperationMessage, is(instanceOf(originalMessage.getClass())));
    return (M) datasetOperationMessage;
  }

  @SuppressWarnings("unchecked")
  protected <M extends DatasetEntityResponse> M encodeDecode(M response) throws MessageCodecException {
    byte[] bytes = datasetMessageCodec.encodeResponse(response);
    DatasetEntityResponse datasetEntityResponse = datasetMessageCodec.decodeResponse(bytes);
    assertThat(datasetEntityResponse.getType(), is(response.getType()));
    assertThat(datasetEntityResponse, is(instanceOf(response.getClass())));
    return (M)datasetEntityResponse;
  }

  protected static <T extends Exception> void assertThrows(Callable<?> proc, Class<T> expected) throws Exception {
    try {
      proc.call();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  protected static Collection<Cell<?>> createCells() {
    return Arrays.asList(
            CellDefinition.define("human", Type.BOOL).newCell(true),
            CellDefinition.define("gender", Type.CHAR).newCell('F'),
            CellDefinition.define("streetNum", Type.INT).newCell(Integer.MIN_VALUE),
            CellDefinition.define("timeBeforeRetirement", Type.LONG).newCell(Long.MAX_VALUE),
            CellDefinition.define("avg", Type.DOUBLE).newCell(Double.NaN),
            CellDefinition.define("address", Type.STRING).newCell("some street, somewhere"),
            CellDefinition.define("data", Type.BYTES).newCell(new byte[] {0, 1, 2, 3, 4, 5}));
  }
}
