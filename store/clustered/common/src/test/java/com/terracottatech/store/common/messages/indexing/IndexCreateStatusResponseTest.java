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

import org.junit.Test;

import com.terracottatech.store.common.messages.BaseMessageTest;
import com.terracottatech.store.indexing.Index;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link IndexCreateStatusResponse} message.
 */
public class IndexCreateStatusResponseTest extends BaseMessageTest {

  @Test
  public void testNullArg() throws Exception {
    try {
      new IndexCreateStatusResponse((Index.Status) null);
      fail("Expecting NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    try {
      new IndexCreateStatusResponse((Throwable) null);
      fail("Expecting NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Override
  public void testEncodeDecode() throws Exception {
    IndexCreateStatusResponse createResponse = new IndexCreateStatusResponse(Index.Status.LIVE);
    IndexCreateStatusResponse decodedCreateResponse = encodeDecode(createResponse);
    assertThat(decodedCreateResponse.getStatus(), is(Index.Status.LIVE));
    assertThat(decodedCreateResponse.getFault(), is(nullValue()));

    createResponse = new IndexCreateStatusResponse(new Throwable("failed"));
    decodedCreateResponse = encodeDecode(createResponse);
    assertThat(decodedCreateResponse.getStatus(), is(nullValue()));
    assertThat(decodedCreateResponse.getFault(), is(instanceOf(Throwable.class)));
    assertThat(decodedCreateResponse.getFault().getMessage(), is("failed"));
  }
}