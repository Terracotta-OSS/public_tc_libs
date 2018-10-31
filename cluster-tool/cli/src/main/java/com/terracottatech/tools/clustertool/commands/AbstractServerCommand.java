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

import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.utilities.HostAndIpValidator.isValidHost;
import static com.terracottatech.utilities.HostAndIpValidator.isValidIPv4;
import static com.terracottatech.utilities.HostAndIpValidator.isValidIPv6;

/**
 * Class containing common utility methods for cluster tool commands, and implementing
 * {@link Command}, to serve as the common parent for all cluster tool commands.
 */
public abstract class AbstractServerCommand implements Command {

  /**
   * Accepts two lists containing host:ports, performs non-empty checks, and uses the single non-empty list to perform
   * host and IP address validations.
   *
   * @param hostPortList_1 first list to be validated
   * @param hostPortList_2 second list to be validated
   * @return a non-empty list with IPv6 addresses enclosed in square brackets.
   * @throws ParameterException if the list contains invalid input
   */
  List<String> validateAndGetHostPortList(List<String> hostPortList_1, List<String> hostPortList_2) {
    List<String> hostPortList;

    if (hostPortList_2.isEmpty() && hostPortList_1.isEmpty()) {
      //Both lists empty
      throw new ParameterException("Invalid Parameter: At least one server <host>, or <host>:<port> must be provided.");
    } else if (hostPortList_2.isEmpty()) {
      hostPortList = hostPortList_1;
    } else if (hostPortList_1.isEmpty()) {
      hostPortList = hostPortList_2;
    } else {
      //Both lists non-empty, forbid mixed usage
      throw new ParameterException("Invalid Parameter: Servers should be separated by '-s' parameter.");
    }

    return validateHostPortList(hostPortList);
  }

  List<String> validateHostPortList(List<String> hostPortList) {
    List<String> listToReturn = new ArrayList<>();
    for (String hostPort : hostPortList) {
      String hostOrIp;
      int port;

      if (isValidIPv6(hostPort, false)) {
        /*
         * We store IPv6 addresses as addresses enclosed in square brackets so that:
         * 1. ConnectionService can use this hostPort as is to establish a connection
         * 2. Other classes (like Server) which store IPv6 addresses don't have to perform extra operations
         */
        hostPort = "[" + hostPort + "]";
      } else if (!isValidHost(hostPort) && !isValidIPv4(hostPort) && !isValidIPv6(hostPort, true)) {
        //We'll come here only if an invalid input, or a valid host:port was specified
        if (!hostPort.contains(":")) {
          throw new ParameterException("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address");
        } else {
          hostOrIp = hostPort.substring(0, hostPort.lastIndexOf(":"));

          /*
           * Check the validity of the host name or the IP address.
           * IPv6 address when specified with a port, is enclosed in square brackets (e.g. [::1]:9510).
           */
          if (!isValidHost(hostOrIp) && !isValidIPv4(hostOrIp) && !isValidIPv6(hostOrIp, true)) {
            throw new ParameterException("Invalid Parameter: <host> must be an RFC 1123 compliant hostname or a valid IP address");
          }

          //Now check the validity of the port
          try {
            port = Integer.parseInt(hostPort.substring(hostPort.lastIndexOf(":") + 1));
          } catch (NumberFormatException e) {
            throw new ParameterException("Invalid Parameter: <port> must be an integer between 1 and 65535");
          }
          if (port < 1 || port > 65535) {
            throw new ParameterException("Invalid Parameter: <port> must be an integer between 1 and 65535");
          }
        }
      }
      listToReturn.add(hostPort);
    }
    return listToReturn;
  }

  boolean isClusterNameSpecified(String clusterName) {
    return clusterName != null && !clusterName.isEmpty();
  }

  boolean processHelp(boolean help, JCommander jCommander) {
    if (help) {
      jCommander.usage(name());
      return true;
    }
    return false;
  }
}
