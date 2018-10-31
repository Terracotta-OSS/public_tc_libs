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

package com.terracottatech.sovereign.common.utils;

import com.terracottatech.sovereign.impl.utils.CachingSequence;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CachingSequenceTest {

  @Test
  public void testSingleThreaded() {
    AtomicInteger atom = new AtomicInteger(0);
    CachingSequence seq = new CachingSequence(0, 10);
    seq.setCallback(r -> {
    });
    for (int i = 0; i < 101; i++) {
      assertThat(seq.next(), is((long) i + 1l));
    }
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    AtomicInteger atom = new AtomicInteger(0);
    CachingSequence seq = new CachingSequence(0, 10);
    seq.setCallback(r -> {
    });
    for (int i = 0; i < 101; i++) {
      assertThat(seq.next(), is((long) i + 1l));
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(seq);
    oos.flush();
    oos.close();
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    CachingSequence seq2 = (CachingSequence) ois.readObject();
    seq2.setCallback(r -> {
    });
    assertThat(seq2.current(), is(110l));

    for (int i = 0; i < 100; i++) {
      assertThat(seq2.next(), is((long) i + 111l));
    }
  }

  @Test
  public void noWriteExternalLock() throws Exception {
    CountDownLatch callback = new CountDownLatch(1);
    CountDownLatch written = new CountDownLatch(1);
    CachingSequence sequence  = new CachingSequence(0, 10);

    Thread nextThread = new Thread(sequence::next);
    sequence.setCallback(seq -> {
      try {
        callback.countDown();
        written.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    });

    Thread writeThread = new Thread(() -> {
      try {
        callback.await();
        ObjectOutput out = new ObjectOutputStream(new ByteArrayOutputStream());
        sequence.writeExternal(out);
        written.countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    nextThread.start();
    writeThread.start();

    nextThread.join(10_000);
    writeThread.join(10_000);

    try {
      assertEquals(0, callback.getCount());
      assertEquals(0, written.getCount());
    } finally {
      callback.countDown();
      written.countDown();
    }
  }
}
