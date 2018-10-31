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

package com.terracottatech.store.embedded.demo;

import org.junit.Test;

import java.io.BufferedReader;

import static com.terracottatech.tool.DriverSupport.capturingOutput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Drives {@link RoomReservationDemo} as a unit test.
 */
public class RoomReservationDemoDriver {

  @Test
  public void testMain() throws Exception {
    BufferedReader[] streams = capturingOutput(() -> RoomReservationDemo.main(new String[0]));
    try (BufferedReader out = streams[0]; BufferedReader err = streams[1]) {
      String[] stdout = out.lines().toArray(String[]::new);
      assertThat(stdout, hasItemInArray(containsString("Reserved by Albin")));

      String[] stderr = err.lines().toArray(String[]::new);
      assertThat(stderr, is(emptyArray()));
    }
  }
}