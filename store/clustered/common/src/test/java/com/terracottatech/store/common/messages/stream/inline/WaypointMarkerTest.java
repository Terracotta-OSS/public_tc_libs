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
package com.terracottatech.store.common.messages.stream.inline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link WaypointMarker} classes.
 */
public class WaypointMarkerTest {

  @Test
  public void testWaypointMarker() {
    Object marker = new WaypointMarker<String>(0);
    assertEquals(marker, marker);
    assertEquals(marker.hashCode(), marker.hashCode());

    Object same = new WaypointMarker<String>(0);
    assertEquals(marker, same);
    assertEquals(marker.hashCode(), same.hashCode());

    Object otherLeft = new WaypointMarker<String>(1);
    assertNotEquals(marker, otherLeft);
  }
}
