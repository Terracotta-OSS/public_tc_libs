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
package com.terracottatech.testing.master;


import org.terracotta.testing.master.GalvanFailureException;
import org.terracotta.testing.master.IMultiProcessControl;

import java.util.Arrays;

/**
 * This just wraps a list of IMultiProcessControl implementations, relaying all commands to all of them.
 * Additionally, since we still want the entire cluster to be controlled in lock-step, we will synchronize every method.
 */
public class MultiStripeProcessControl implements IMultiProcessControl {
  private final IMultiProcessControl[] subControl;

  public MultiStripeProcessControl(IMultiProcessControl[] subControl) {
    this.subControl = Arrays.copyOf(subControl, subControl.length);
  }

  @Override
  public synchronized void synchronizeClient() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.synchronizeClient();
    }
  }

  @Override
  public void terminateActive() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.terminateActive();
    }
  }

  @Override
  public void terminateOnePassive() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.terminateOnePassive();
    }
  }

  @Override
  public void startOneServer() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.startOneServer();
    }
  }

  @Override
  public void startAllServers() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.startAllServers();
    }
  }

  @Override
  public synchronized void terminateAllServers() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.terminateAllServers();
    }
  }

  @Override
  public synchronized void waitForActive() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.waitForActive();
    }
  }

  @Override
  public synchronized void waitForRunningPassivesInStandby() throws GalvanFailureException {
    for (IMultiProcessControl oneControl : this.subControl) {
      oneControl.waitForRunningPassivesInStandby();
    }
  }
}
