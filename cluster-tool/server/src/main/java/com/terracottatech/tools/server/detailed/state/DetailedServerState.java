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
package com.terracottatech.tools.server.detailed.state;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.classloader.CommonComponent;
import com.tc.management.AbstractTerracottaMBean;
import com.tc.management.TerracottaManagement;
import com.tc.objectserver.impl.JMXSubsystem;
import com.terracottatech.tools.detailed.state.LogicalServerState;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

@CommonComponent
public class DetailedServerState {

  private static final Logger LOGGER = LoggerFactory.getLogger(DetailedServerState.class);
  private static final String DETAILED_SERVER_STATE_MBEAN_NAME = "DetailedServerState";
  private static final String CONSISTENCY_MANAGER_MBEAN_NAME = "ConsistencyManager";
  private static final String SERVER_MBEAN_NAME = "Server";
  private SubSystemCaller subSystemCaller;

  public void init() {
    if (!isReachable(DETAILED_SERVER_STATE_MBEAN_NAME)) {
      this.subSystemCaller = new JmxSubSystemCaller();
    }
  }

  private static boolean isReachable(String detailedServerStateMbeanName) {
    try {
      Set<ObjectInstance> matchingBeans = ManagementFactory.getPlatformMBeanServer().queryMBeans(
          TerracottaManagement.createObjectName(null, detailedServerStateMbeanName, TerracottaManagement.MBeanDomain.PUBLIC), null);
      return matchingBeans.iterator().hasNext();
    } catch (MalformedObjectNameException e) {
      // really not supposed to happen
      LOGGER.error("Invalid MBean name : " + detailedServerStateMbeanName, e);
      return false;
    }
  }

  static LogicalServerState retrieveDetailedServerState(SubSystemCaller subSystemCaller) {
    boolean isBlocked;
    if (subSystemCaller.hasConsistencyManager()) {
      isBlocked = Boolean.valueOf(subSystemCaller.isBlocked());
    } else {
      isBlocked = false;
    }
    boolean isReconnectWindow = Boolean.valueOf(subSystemCaller.isReconnectWindow());
    String state = subSystemCaller.getState();

    return enhanceServerState(state, isReconnectWindow, isBlocked);
  }

  private static LogicalServerState enhanceServerState(String state, boolean reconnectWindow, boolean isBlocked) {
    // subsystem call failed
    if (state == null || state.startsWith("Invalid JMX call")) {
      LOGGER.error("A server returned the following invalid state : ", state == null ? "null" : state);
      return LogicalServerState.UNKNOWN;
    }

    // enhance default server state
    return LogicalServerState.from(state, reconnectWindow, isBlocked);
  }


  public class DetailedServerStateMbeanImpl extends AbstractTerracottaMBean implements DetailedServerStateMbean {

    DetailedServerStateMbeanImpl() throws Exception {
      super(DetailedServerStateMbean.class, false);
    }

    @Override
    public String getDetailedServerState() {
      return retrieveDetailedServerState(subSystemCaller).name();
    }

    @Override
    public void reset() {
      // nothing
    }
  }

  class JmxSubSystemCaller implements SubSystemCaller {
    private final JMXSubsystem subsystem;

    JmxSubSystemCaller() {
      subsystem = new JMXSubsystem();
      initMBean();
    }

    private void initMBean() {
      try {
        ObjectName mBeanName = TerracottaManagement.createObjectName(null, DETAILED_SERVER_STATE_MBEAN_NAME, TerracottaManagement.MBeanDomain.PUBLIC);
        ManagementFactory.getPlatformMBeanServer().registerMBean(new DetailedServerStateMbeanImpl(), mBeanName);
      } catch (Exception e) {
        LOGGER.warn("DetailedServerState MBean not initialized", e);
      }
    }

    @Override
    public String isBlocked() {
      return subsystem.call(CONSISTENCY_MANAGER_MBEAN_NAME, "isBlocked", null);
    }

    @Override
    public String isReconnectWindow() {
      return subsystem.call(SERVER_MBEAN_NAME, "isReconnectWindow", null);
    }

    @Override
    public String getState() {
      return subsystem.call(SERVER_MBEAN_NAME, "getState", null);
    }

    @Override
    public boolean hasConsistencyManager() {
      // JMX query to find the consistency manager
      return isReachable(CONSISTENCY_MANAGER_MBEAN_NAME);
    }
  }

}
