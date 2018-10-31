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
package com.terracottatech.tools.clustertool.commands;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class AbstractServerCommandTest {
  private AbstractServerCommand dumbServerCommand = new AbstractServerCommand() {
    @Override
    public String name() {
      return "wesh";
    }

    @Override
    public void process(JCommander jCommander) {}

    @Override
    public String usage() {
      return "";
    }
  };

  @Test
  public void validateHostPortList_ok_hostname() {
    List<String> validHosts = dumbServerCommand.validateHostPortList(
        Arrays.asList(
            "some-host:9510", //Hostname containing '-' and port
            "some-host", //Hostname containing '-' only
            "pif:9610", //Simple hostname and port
            "piaf22:9710" //Hostname containing numbers and port
        ));
    assertThat(validHosts, hasItems("some-host:9510", "some-host", "pif:9610", "piaf22:9710"));
  }

  @Test
  public void validateHostPortList_ok_ipv4_lookalike_hostname() {
    List<String> validHosts = dumbServerCommand.validateHostPortList(
        Arrays.asList(
            "10.10.10",
            "10.10.10.10.10",
            "999.10.10.400",
            "10-10-10-10"
        ));
    assertThat(validHosts, hasItems("10.10.10", "10.10.10.10.10", "999.10.10.400", "10-10-10-10"));
  }

  @Test
  public void validateHostPortList_notok_hostname() {
    try {
      dumbServerCommand.validateHostPortList(Collections.singletonList("some_host"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      dumbServerCommand.validateHostPortList(Collections.singletonList("%%%@@^^***"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }
  }

  @Test
  public void validateHostPortList_ok_ipv4() {
    List<String> validIPs = dumbServerCommand.validateHostPortList(
        Arrays.asList(
            "10.12.14.43:9610", //Address and port
            "10.12.14.43", //Address only
            "127.0.0.1:9510", //Loopback address and port
            "127.0.0.1" //Loopback address only
        ));
    assertThat(validIPs, hasItems("10.12.14.43:9610", "10.12.14.43", "127.0.0.1:9510", "127.0.0.1"));
  }

  @Test
  public void validateHostPortList_notok_ipv4() {
    try {
      //Correct length but invalid characters
      dumbServerCommand.validateHostPortList(Collections.singletonList("abc.10.10.%%^"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Empty input
      dumbServerCommand.validateHostPortList(Collections.singletonList(""));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Only port
      dumbServerCommand.validateHostPortList(Collections.singletonList(":9510"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }
  }

  @Test
  public void validateHostPortList_ok_ipv6() {
    List<String> validIPs = dumbServerCommand.validateHostPortList(
        Arrays.asList(
            "[2001:db8:a0b:12f0:0:0:0:1]:9510", //Full address and port
            "2001:db8:a0b:12f0:0:0:0:1", //Full address only
            "[2001:db8:a0b:12f0:0:0:0:1]", //Full address enclosed in brackets
            "[2001:db8:a0b:12f0::1]:9510", //Shortened address and port
            "2001:db8:a0b:12f0::1", //Shortened address only
            "[::1]:9510", //Loopback address with port
            "::1", //Loopback address only
            "[::1]", //Loopback address enclosed in brackets
            "[2001:db8::]:9510", //Funny address with port
            "2001:db8::" //Funny address only
        ));

    assertThat(validIPs, hasItems("[2001:db8:a0b:12f0:0:0:0:1]:9510", "[2001:db8:a0b:12f0:0:0:0:1]", "[2001:db8:a0b:12f0:0:0:0:1]",
        "[2001:db8:a0b:12f0::1]:9510", "[2001:db8:a0b:12f0::1]", "[::1]:9510", "[::1]", "[::1]", "[2001:db8::]:9510", "[2001:db8::]"));
  }

  @Test
  public void validateHostPortList_notok_ipv6() {
    try {
      //Too short
      dumbServerCommand.validateHostPortList(Collections.singletonList("aaaa:aaaa:aaaa:aaaa"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Too long
      dumbServerCommand.validateHostPortList(Collections.singletonList("aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa:aaaa"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Correct length but invalid characters
      dumbServerCommand.validateHostPortList(Collections.singletonList("zzzz:aaaa:aaaa:nnnn:aaaa:aaaa:aaaa:zzzz"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Multiple double colons
      dumbServerCommand.validateHostPortList(Collections.singletonList("2001:db8:a0b::12f0::1"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Triple colon
      dumbServerCommand.validateHostPortList(Collections.singletonList("2001:db8:a0b:12f0:::1"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //IPv6 address and port without enclosing the IP in square brackets
      dumbServerCommand.validateHostPortList(Collections.singletonList("2001:1:1:1:1:1:1:1:9510"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Only port
      dumbServerCommand.validateHostPortList(Collections.singletonList("[]:9510"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }
  }

  @Test
  public void validateHostPortList_notok_too_many_ports() {
    try {
      //Too many ports with hostname
      dumbServerCommand.validateHostPortList(Collections.singletonList("host:12:12:1212"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Too many ports with IPv4 address
      dumbServerCommand.validateHostPortList(Collections.singletonList("10.10.10.10:12:12"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }

    try {
      //Too many ports with IPv6 address
      dumbServerCommand.validateHostPortList(Collections.singletonList("2001:db8:a0b:12f0:0:0:0:1:12:12"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address"));
    }
  }

  @Test
  public void validateHostPortList_notok_faulty_port() {
    try {
      //Port out of allowed range
      dumbServerCommand.validateHostPortList(Collections.singletonList("host:12121212"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <port> must be an integer between 1 and 65535"));
    }

    try {
      //Port out of allowed range
      dumbServerCommand.validateHostPortList(Collections.singletonList("host:-1"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <port> must be an integer between 1 and 65535"));
    }

    try {
      //port not a number
      dumbServerCommand.validateHostPortList(Collections.singletonList("host:blah"));
      fail();
    } catch (ParameterException e) {
      assertThat(e.getMessage(), containsString("Invalid Parameter: <port> must be an integer between 1 and 65535"));
    }
  }
}