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

package com.terracottatech.frs.log;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mscott
 */
public class CommitListFuture implements Future<Void> {
  
  final private CommitList origin;
  final private long lsn;

  public CommitListFuture(long lsn, CommitList origin) {
    this.origin = origin;
    this.lsn = lsn;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    CommitList target = origin;
    while ( target.isDone() ) {
      if ( target.getEndLsn() >= lsn ) {
        return true;
      } else {
        target = target.next();
      }
    }
    return false;
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    CommitList target = origin;
    target.get();
    while ( target.getEndLsn() < lsn ) {
      target = target.next();
      target.get();
    }
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    CommitList target = origin;
    long to = unit.toNanos(timeout) + System.nanoTime();
    target.get(timeout,unit);
    while ( target.getEndLsn() < lsn ) {
      target = target.next();
      long ct = System.nanoTime();
      if ( ct > to ) {
        throw new TimeoutException();
      }
      target.get(to - ct, TimeUnit.NANOSECONDS);

    }
    return null;
  }
  
}
