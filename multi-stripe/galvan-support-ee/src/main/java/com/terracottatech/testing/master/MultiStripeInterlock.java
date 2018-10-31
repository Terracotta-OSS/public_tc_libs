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

import org.terracotta.testing.master.ClientRunner;
import org.terracotta.testing.master.GalvanFailureException;
import org.terracotta.testing.master.GalvanStateInterlock;
import org.terracotta.testing.master.IGalvanStateInterlock;
import org.terracotta.testing.master.ServerProcess;

import java.util.Arrays;


/**
 * The interlock for multiple stripes is somewhat different than the single-stripe variant.
 * The servers are still logically managed per-stripe so we defer to the per-stripe interlocks for most of what we
 *  are doing, but the clients are managed per-cluster, so we manage those at this level.
 *
 * Note that this is an example of the extensibility afforded by the ITestWaiter sharedLockState:  we can share the
 *  lock-state of the per-stripe interlocks, while remaining logically distinct.
 *
 */
public class MultiStripeInterlock implements IGalvanStateInterlock {
  private final GalvanStateInterlock[] stripeInterlocks;
  private final GalvanStateInterlock clientInterlock;


  public MultiStripeInterlock(GalvanStateInterlock[] stripeInterlocks, GalvanStateInterlock clientInterlock) {
    this.stripeInterlocks = Arrays.copyOf(stripeInterlocks, stripeInterlocks.length);
    this.clientInterlock = clientInterlock;
  }

  @Override
  public void registerNewServer(ServerProcess newServer) {
    // We can't register a server in this aggregate representation.
    throw new AssertionError("Path should not be reachable");  }

  @Override
  public void registerRunningClient(ClientRunner runningClient) throws GalvanFailureException {
    this.clientInterlock.registerRunningClient(runningClient);
  }

  @Override
  public void waitForClientTermination() throws GalvanFailureException {
    this.clientInterlock.waitForClientTermination();
  }

  @Override
  public void waitForActiveServer() throws GalvanFailureException {
    for (IGalvanStateInterlock stripe : stripeInterlocks) {
      stripe.waitForActiveServer();
    }
  }

  @Override
  public void waitForServerRunning(ServerProcess startingUpServer) throws GalvanFailureException {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");  }

  @Override
  public void waitForServerTermination(ServerProcess terminatedServer) throws GalvanFailureException {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");  }

  @Override
  public void waitForAllServerRunning() throws GalvanFailureException {
    for (IGalvanStateInterlock stripe : stripeInterlocks) {
      stripe.waitForAllServerRunning();
    }
  }

  @Override
  public void waitForAllServerReady() throws GalvanFailureException {
    for (IGalvanStateInterlock stripe : stripeInterlocks) {
      stripe.waitForAllServerReady();
    }
  }

  @Override
  public ServerProcess getActiveServer() throws GalvanFailureException {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public ServerProcess getOnePassiveServer() throws GalvanFailureException {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public ServerProcess getOneTerminatedServer() throws GalvanFailureException {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");  }

  @Override
  public void serverBecameActive(ServerProcess server) {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public void serverBecamePassive(ServerProcess server) {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public void serverDidShutdown(ServerProcess server) {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public void serverWasZapped(ServerProcess server) {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public void serverDidStartup(ServerProcess server) {
    // We can't interact with specific servers in an aggregate representation.
    throw new AssertionError("Path should not be reachable");
  }

  @Override
  public void clientDidTerminate(ClientRunner client) {
    this.clientInterlock.clientDidTerminate(client);
  }

  @Override
  public void forceShutdown() throws GalvanFailureException {
    if (clientInterlock != null) {
      this.clientInterlock.forceShutdown();
    }
    for (IGalvanStateInterlock stripe : stripeInterlocks) {
      stripe.forceShutdown();
    }
  }

  public void ignoreServerCrashes(boolean ignoreCrash) {
    for (GalvanStateInterlock stripeInterlock : stripeInterlocks) {
      stripeInterlock.ignoreServerCrashes(ignoreCrash);
    }
  }
}
