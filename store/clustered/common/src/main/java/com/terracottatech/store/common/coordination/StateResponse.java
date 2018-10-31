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
package com.terracottatech.store.common.coordination;

import org.terracotta.entity.EntityResponse;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public final class StateResponse implements EntityResponse {

  private final Set<String> members;
  private final LeadershipState leader;

  public StateResponse(Collection<String> members, LeadershipState leadership) {
    this.members = unmodifiableSet(new HashSet<>(members));
    this.leader = leadership;
  }

  public Set<String> getMembers() {
    return members;
  }

  public Optional<LeadershipState> getLeader() {
    return Optional.ofNullable(leader);
  }

  public static final class LeadershipState {

    private final String name;
    private final Instant expiry;

    public LeadershipState(String leader, Instant expiry) {
      this.name = leader;
      this.expiry = expiry;
    }

    public String getName() {
      return name;
    }

    public Instant getExpiry() {
      return expiry;
    }
  }
}
