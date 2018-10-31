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
package com.terracottatech.sovereign.impl.persistence;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class SharedSyncherTest {

  @Test
  public void testImmediate() throws InterruptedException {
    AtomicInteger test = new AtomicInteger(0);
    SharedSyncher sync = new SharedSyncher(() -> { test.incrementAndGet();});
    sync.requestImmediate();
    Assert.assertThat(test.get(), is(1));
  }

  @Test
  public void testSingleTimer() throws InterruptedException {
    AtomicInteger test = new AtomicInteger(0);
    SharedSyncher sync = new SharedSyncher(() -> { test.incrementAndGet();});

    SharedSyncher.SyncRequest req = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    Assert.assertThat(sync.getLiveRequestCount(), is(1));

    long r = sync.getRunCounter();
    req.request();
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(1));

    r = sync.getRunCounter();
    req.request();
    req.request(); // this one will be ignored
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(2));
    Assert.assertThat(sync.getLiveRequestCount(), is(1));

    req.release();
    Assert.assertThat(sync.getLiveRequestCount(), is(0));
  }

  private void awaitRuns(SharedSyncher sync, long l) throws InterruptedException {
    for (; ; ) {
      long p = sync.getRunCounter();
      if (p >= l) {
        Thread.sleep(10);
        break;
      }
      Thread.sleep(10);
    }
  }

  @Test
  public void testMultipleSameLength() throws InterruptedException {
    AtomicInteger test = new AtomicInteger(0);
    SharedSyncher sync = new SharedSyncher(() -> { test.incrementAndGet();});

    SharedSyncher.SyncRequest req1 = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    SharedSyncher.SyncRequest req2 = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    Assert.assertThat(sync.getLiveRequestCount(), is(2));

    long r = sync.getRunCounter();
    req1.request();
    req2.request();
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(1));
  }

  @Test
  public void testRelease() throws InterruptedException {
    AtomicInteger test = new AtomicInteger(0);
    SharedSyncher sync = new SharedSyncher(() -> { test.incrementAndGet();});

    SharedSyncher.SyncRequest req1 = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    SharedSyncher.SyncRequest req2 = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    SharedSyncher.SyncRequest req3 = sync.fetchSyncRequest(1000, TimeUnit.MILLISECONDS);
    Assert.assertThat(sync.getLiveRequestCount(), is(3));

    req1.release();
    Assert.assertThat(sync.getLiveRequestCount(), is(2));
  }

  @Test
  public void testMultipleDifferent() throws InterruptedException {
    AtomicInteger test = new AtomicInteger(0);
    SharedSyncher sync = new SharedSyncher(() -> { test.incrementAndGet();});

    SharedSyncher.SyncRequest req1 = sync.fetchSyncRequest(100, TimeUnit.MILLISECONDS);
    SharedSyncher.SyncRequest req2 = sync.fetchSyncRequest(200, TimeUnit.MILLISECONDS);
    SharedSyncher.SyncRequest req3 = sync.fetchSyncRequest(300, TimeUnit.MILLISECONDS);
    Assert.assertThat(sync.getLiveRequestCount(), is(3));
    Assert.assertThat(sync.getCurrentMultipleLengthNS(), is(TimeUnit.MILLISECONDS.toNanos(100)));

    // single smallest one
    long r = sync.getRunCounter();
    req1.request();
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(1));

    // request longest
    r = sync.getRunCounter();
    req3.request();
    awaitRuns(sync, r + 1);

    // request all in order; only first will trigger
    r = sync.getRunCounter();
    req1.request();
    req2.request();
    req3.request();
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(3));

    // request all in reverse order; only first will trigger
    r = sync.getRunCounter();
    req3.request();
    req2.request();
    req1.request();
    awaitRuns(sync, r + 1);
    Assert.assertThat(test.get(), is(4));

  }
}