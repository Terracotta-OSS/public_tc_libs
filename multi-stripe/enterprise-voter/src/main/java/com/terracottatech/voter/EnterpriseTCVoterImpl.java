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
package com.terracottatech.voter;

import org.terracotta.voter.ActiveVoter;
import org.terracotta.voter.TCVoterImpl;
import org.terracotta.voter.VoterStatus;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import com.terracottatech.tools.client.TopologyEntityProvider;
import com.terracottatech.tools.config.Cluster;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddresses;
import static java.util.stream.Collectors.toList;

public class EnterpriseTCVoterImpl extends TCVoterImpl {

  private final TopologyEntityProvider topologyEntityProvider =  new TopologyEntityProvider();
  private final Map<String, List<ActiveVoter>> registeredClusters = new ConcurrentHashMap<>();
  private final Path securityRootDirectory;

  public EnterpriseTCVoterImpl() {
    this(null);
  }

  public EnterpriseTCVoterImpl(Path securityRootDirectory) {
    super();
    this.securityRootDirectory = securityRootDirectory;
  }

  @Override
  protected Optional<Properties> getConnectionProperties() {
    if (securityRootDirectory != null) {
      Properties properties = super.getConnectionProperties().orElse(new Properties());
      properties.setProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootDirectory.toString());
      return Optional.of(properties);
    }
    return Optional.empty();
  }

  @Override
  public Future<VoterStatus> register(String clusterName, String... hostPorts) {
    Cluster cluster;
    try(TopologyEntityProvider.ConnectionCloseableTopologyEntity topologyEntity =
        topologyEntityProvider.getEntity(getInetSocketAddresses(hostPorts), securityRootDirectory != null ? securityRootDirectory.toString() : null)) {
      cluster = topologyEntity.getTopologyEntity().getClusterConfiguration().getCluster();
    } catch (IOException e) {
      throw new RuntimeException("Cluster is not configured. Please configure the cluster first.");
    }

    List<ActiveVoter> voters = new ArrayList<>(cluster.getStripes().size());
    List<CompletableFuture<VoterStatus>> voterStatuses = new ArrayList<>(cluster.getStripes().size());
    for (Stripe stripe : cluster.getStripes()) {
      CompletableFuture<VoterStatus> stripeVoterStatus = new CompletableFuture<>();
      voterStatuses.add(stripeVoterStatus);
      String[] stripeHostPorts = stripe.getServers().stream().map(Server::getHostPort).collect(toList()).toArray(new String[0]);
      ActiveVoter activeVoter = new ActiveVoter(id, stripeVoterStatus, getConnectionProperties(), stripeHostPorts).start();
      voters.add(activeVoter);
    }

    if (registeredClusters.putIfAbsent(clusterName, voters) != null) {
      throw new RuntimeException("Another cluster is already registered with the name: " + clusterName);
    }

    return new Future<VoterStatus>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return voterStatuses.stream().allMatch(CompletableFuture::isDone);
      }

      @Override
      public VoterStatus get() {
        CompletableFuture.allOf(voterStatuses.toArray(new CompletableFuture<?>[voterStatuses.size()])).join();
        return new VoterStatus() {
          @Override
          public boolean isActive() {
            return voterStatuses.stream().allMatch(cf -> {
              try {
                return cf.get().isActive();
              } catch (InterruptedException | ExecutionException e) {
                return false;
              }
            });
          }

          @Override
          public void awaitRegistrationWithAll() throws InterruptedException {
            for (CompletableFuture<VoterStatus> voterStatus : voterStatuses) {
              try {
                voterStatus.get().awaitRegistrationWithAll();
              } catch (ExecutionException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public void awaitRegistrationWithAll(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            AtomicBoolean interrupted = new AtomicBoolean(false);
            try {
              CompletableFuture.runAsync(() -> {
                try {
                  awaitRegistrationWithAll();
                } catch (InterruptedException e) {
                  interrupted.set(true);
                }
              }).get(timeout, unit);
            } catch (ExecutionException e) {
              throw new RuntimeException(e);
            }

            if (interrupted.get()) {
              throw new InterruptedException("Wait for registration got interrupted");
            }
          }
        };
      }

      @Override
      public VoterStatus get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<VoterStatus> voterStatusCompletableFuture = CompletableFuture.supplyAsync(this::get);
        return voterStatusCompletableFuture.get(timeout, unit);
      }
    };
  }

  @Override
  public void deregister(String clusterName) {
    List<ActiveVoter> voters = registeredClusters.remove(clusterName);
    if (voters != null) {
      for (AutoCloseable voter : voters) {
        try {
          voter.close();
        } catch (Exception exp) {
          throw new RuntimeException(exp);
        }
      }
    } else {
      throw new RuntimeException("A cluster with the given name: " + clusterName + " is not registered with this voter");
    }
  }

}
