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
package com.terracottatech.store.client;

import com.terracottatech.store.StoreRuntimeException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FirstServerSideExceptionHelperTest {
  @Test
  public void noCause() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException("Message"));
    assertEquals("Message", message);
  }

  @Test
  public void withCause() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException("Other", new RuntimeException("Message")));
    assertEquals("Message", message);
  }

  @Test
  public void nestedCause() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException("Other", new RuntimeException("Other", new RuntimeException("Message"))));
    assertEquals("Message", message);
  }

  @Test
  public void stripsStoreClassPrefix() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException(StoreRuntimeException.class.getName() + ": Message"));
    assertEquals("Message", message);
  }

  @Test
  public void stripsJDKClassPrefix() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException(RuntimeException.class.getName() + ": Message"));
    assertEquals("Message", message);
  }

  @Test
  public void doesNotStripNonClassName() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException("thing: value"));
    assertEquals("thing: value", message);
  }

  @Test
  public void doesNotStripNonClassNameEvenWithDot() {
    String message = FirstServerSideExceptionHelper.getMessage(new RuntimeException("other.thing: value"));
    assertEquals("other.thing: value", message);
  }
}
