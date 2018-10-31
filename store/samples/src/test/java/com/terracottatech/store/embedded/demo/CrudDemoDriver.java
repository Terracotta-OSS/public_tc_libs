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
import java.util.List;

import static com.terracottatech.tool.DriverSupport.capturingOutput;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Drives {@link CrudDemo} as a unit test.
 */
public class CrudDemoDriver {

  private static final boolean SHOW_OUTPUT = false;

  @Test
  public void testMain() throws Exception {

    List<String> stdout;
    List<String> stderr;
    BufferedReader[] streams = capturingOutput(SHOW_OUTPUT, () -> CrudDemo.main(new String[0]));
    try (BufferedReader out = streams[0]; BufferedReader err = streams[1]) {
      stdout = out.lines().collect(toList());
      stderr = err.lines().collect(toList());
    }

    assertThat(stdout, hasItem(containsString(" lastName of type: Type<String> has value: Aurelius")));
    assertThat(stdout, hasItem(containsString(" niceness of type: Type<Double> has value: 0.65")));
    assertThat(stdout, hasItem(containsString("Found a nice person: Optional[MacDonald]")));
    assertThat(stdout, hasItem(containsString("Total number of people is now: 2")));

    assertThat(stderr, is(empty()));
  }
}