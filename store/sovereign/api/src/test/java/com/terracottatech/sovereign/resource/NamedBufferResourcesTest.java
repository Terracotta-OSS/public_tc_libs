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
package com.terracottatech.sovereign.resource;

import com.terracottatech.store.StoreRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NamedBufferResourcesTest {
  @Test(expected = StoreRuntimeException.class)
  public void noResourcesNoReservation() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources);

    assertFalse(namedBufferResources.reserve(1));
  }

  @Test(expected = StoreRuntimeException.class)
  public void noActivationNoReservation() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    SizedBufferResource a = new SizedBufferResource(1);
    bufferResources.put("a", a);
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources);

    assertFalse(namedBufferResources.reserve(1));
    Assert.assertEquals(1, a.getAvailable());
  }

  @Test(expected = StoreRuntimeException.class)
  public void activatingSomethingThatDoesNotExistFails() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    bufferResources.put("a", new SizedBufferResource(1));
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources);

    namedBufferResources.activate("b");
  }

  @Test
  public void afterActivationReservationIsPossible() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    SizedBufferResource a = new SizedBufferResource(1);
    bufferResources.put("a", a);
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources, "a");

    assertTrue(namedBufferResources.reserve(1));
    Assert.assertEquals(0, a.getAvailable());
    assertFalse(namedBufferResources.reserve(1));
    Assert.assertEquals(0, a.getAvailable());
  }

  @Test
  public void cannotReserveToResourcesNotActivated() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    SizedBufferResource a = new SizedBufferResource(1);
    SizedBufferResource b = new SizedBufferResource(1);
    bufferResources.put("a", a);
    bufferResources.put("b", b);
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources, "a");

    assertFalse(namedBufferResources.reserve(2));
    Assert.assertEquals(1, a.getAvailable());
    Assert.assertEquals(1, b.getAvailable());
  }

  @Test(expected = StoreRuntimeException.class)
  public void multipleResourcesConstructorFails() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    SizedBufferResource a = new SizedBufferResource(1);
    SizedBufferResource b = new SizedBufferResource(1);
    bufferResources.put("a", a);
    bufferResources.put("b", b);
    new NamedBufferResources(bufferResources, "a", "b");
  }

  @Test(expected = StoreRuntimeException.class)
  public void multipleResourcesLaterActivationFails() {
    Map<String, SizedBufferResource> bufferResources = new HashMap<>();
    SizedBufferResource a = new SizedBufferResource(1);
    SizedBufferResource b = new SizedBufferResource(1);
    bufferResources.put("a", a);
    bufferResources.put("b", b);
    NamedBufferResources namedBufferResources = new NamedBufferResources(bufferResources);
    namedBufferResources.activate("a");
    namedBufferResources.activate("b");
  }
}
