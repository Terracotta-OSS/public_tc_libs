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
package com.terracottatech.tools.clustertool.managers;

import com.terracottatech.tools.config.Server;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DefaultClusterManagerTest {
  @Test
  public void testParseServers_host() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "localhost",
            "tc-bigmemory-01"
        ));

    assertThat(parsedList.get(0).getName(), is("localhost"));
    assertThat(parsedList.get(1).getName(), is("tc-bigmemory-01"));
  }

  @Test
  public void testParseServers_host_and_port() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "localhost:9510",
            "tc-bigmemory-01:9610"
        ));

    assertThat(parsedList.get(0).getHostPort(), is("localhost:9510"));
    assertThat(parsedList.get(1).getHostPort(), is("tc-bigmemory-01:9610"));
  }

  @Test
  public void testParseServers_ipv4() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "10.10.10.10",
            "127.0.0.1"
        ));

    assertThat(parsedList.get(0).getName(), is("10.10.10.10"));
    assertThat(parsedList.get(1).getName(), is("127.0.0.1"));
  }

  @Test
  public void testParseServers_ipv4_and_port() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "10.10.10.10:9510",
            "127.0.0.1:9610"
        ));

    assertThat(parsedList.get(0).getName(), is("10.10.10.10"));
    assertThat(parsedList.get(0).getHostPort(), is("10.10.10.10:9510"));
    assertThat(parsedList.get(1).getName(), is("127.0.0.1"));
    assertThat(parsedList.get(1).getHostPort(), is("127.0.0.1:9610"));
  }

  @Test
  public void testParseServers_ipv6() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "[10:aa:bb::1]",
            "[::1]"
        ));

    assertThat(parsedList.get(0).getName(), is("[10:aa:bb::1]"));
    assertThat(parsedList.get(1).getName(), is("[::1]"));
  }

  @Test
  public void testParseServers_ipv6_and_port() throws Exception {
    List<Server> parsedList = DefaultClusterManager.parseServers(
        Arrays.asList(
            "[10:aa:bb::1]:9510",
            "[::1]:9610"
        ));

    assertThat(parsedList.get(0).getName(), is("[10:aa:bb::1]"));
    assertThat(parsedList.get(0).getHostPort(), is("[10:aa:bb::1]:9510"));
    assertThat(parsedList.get(1).getName(), is("[::1]"));
    assertThat(parsedList.get(1).getHostPort(), is("[::1]:9610"));
  }
}