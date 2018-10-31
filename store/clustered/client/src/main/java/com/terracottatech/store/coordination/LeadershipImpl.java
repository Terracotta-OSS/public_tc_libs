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
package com.terracottatech.store.coordination;

import com.terracottatech.store.StoreOperationAbandonedException;
import com.terracottatech.store.coordination.Coordinator.LeadershipViolationException;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

class LeadershipImpl implements Coordinator.Leadership {

  private final CoordinationEntity entity;

  LeadershipImpl(CoordinationEntity entity) {
    this.entity = entity;
  }

  @Override
  public synchronized <R> R execute(Callable<R> callable, Duration duration) throws LeadershipViolationException, ExecutionException {
    if (leaseLeadership(duration)) {
      R result;
      try {
        result = callable.call();
      } catch (Exception e) {
        ExecutionException failure = new ExecutionException(e);
        if (checkLeadership()) {
          leaseLeadership(Duration.ZERO);
          throw failure;
        } else {
          throw new LeadershipViolationException("Leadership absent on exit", failure);
        }
      }
      if (checkLeadership()) {
        leaseLeadership(Duration.ZERO);
        return result;
      } else {
        throw new LeadershipViolationException("Leadership absent on exit", result);
      }
    } else {
      throw new LeadershipViolationException("Leadership absent on entry");
    }
  }

  private boolean checkLeadership() {
    while (true) {
      try {
        return entity.leader().map(entity.name()::equals).orElse(false);
      } catch (StoreOperationAbandonedException e) {
        //retry - idempotent operation
      }
    }
  }

  private boolean leaseLeadership(Duration duration) {
    while (true) {
      try {
        return entity.leaseLeadership(duration);
      } catch (StoreOperationAbandonedException e) {
        //retry - idempotent operation
      }
    }
  }

  @Override
  public void close() {
    while (true) {
      try {
        entity.releaseLeadership();
        return;
      } catch (StoreOperationAbandonedException e) {
        //retry - idempotent operation
      }
    }
  }
}
