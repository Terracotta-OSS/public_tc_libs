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

import java.io.StringWriter;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

/**
 * Drives {@link CounterDemo} as a unit test
 */
public class CounterDemoDriver {

  @Test
  public void testMain() throws Exception {
    CounterDemo.main(new String[0]);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFunctionality() throws Exception {
    CounterDemo demo = new CounterDemo();

    StringWriter writer = new StringWriter();

    demo.runEmbedded(writer);
    List<String> lines = asList(writer.toString().split("\n"));

    assertThat(lines, hasItems(
            containsString("'counter9' was stopped by: Albin"),
            containsString(" not stopped yet is: 4.5"),
            containsString(" DSL is: 4.5"),
            containsString("DSL mutation: 'counter8' was stopped by: Albin"),
            containsString(" key counter10 was added")));

    lines.stream()
            .filter(s -> s.startsWith("[postStopping] "))
            .forEach(s -> {
              assertThat(s, stringContainsInOrder(asList("stopped", "true")));
              assertThat(s, stringContainsInOrder(asList("stoppedBy", "Albin")));
            });
  }
}