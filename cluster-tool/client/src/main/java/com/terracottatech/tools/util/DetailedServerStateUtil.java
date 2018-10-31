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
package com.terracottatech.tools.util;

import com.terracotta.diagnostic.Diagnostics;

import static com.terracottatech.tools.detailed.state.LogicalServerState.UNKNOWN;

public class DetailedServerStateUtil {

  static final String INVALID_JMX_CALL = "Invalid JMX call";

  /**
   * Retrieve the best server state from the diagnostics connection
   *
   * @param diagnostics, the client side Diagnostics to invoke
   * @return the best server state possible
   */
  public static String getDetailedServerState(Diagnostics diagnostics) {
    String serverState = diagnostics.invoke("DetailedServerState", "getDetailedServerState");
    //If the MBean wasn't registered yet, return UNKNOWN status
    if (serverState.startsWith(INVALID_JMX_CALL)) {
      // maybe we connect to an old version, 10.2 for example, that does not have this MBean
      // in this case, let's try the original Server state Mbean
      serverState = diagnostics.getState();
      if (serverState.startsWith(INVALID_JMX_CALL)) {
        return UNKNOWN.name();
      }
    }
    return serverState;
  }
}