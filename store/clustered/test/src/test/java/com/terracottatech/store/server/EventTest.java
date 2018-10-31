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
package com.terracottatech.store.server;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.store.server.TestChangeListener.add;
import static com.terracottatech.store.server.TestChangeListener.delete;
import static com.terracottatech.store.server.TestChangeListener.update;
import static com.terracottatech.testing.EnterprisePassthroughConnectionService.SS_SCHEME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class EventTest extends PassthroughTest {

  private final List<String> stripeNames;

  public EventTest(List<String> stripeNames) {
    this.stripeNames = stripeNames;
  }

  @Override
  protected List<String> provideStripeNames() {
    return stripeNames;
  }

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {Collections.singletonList("stripe"), Arrays.asList("stripe1", "stripe2") };
  }

  @Test
  public void getAddEventOnSingleDataset() throws Exception {
    DatasetWriterReader<String> addressAccess = dataset.writerReader();

    TestChangeListener<String> addressListener = new TestChangeListener<>();
    addressAccess.registerChangeListener(addressListener);

    addressAccess.add("abc");

    assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc")));
  }

  @Test
  public void getAddEvent() throws Exception {
    try (Dataset<Long> personDataset = createPersonDataset()) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      personAccess.add(123L);

      assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc")));
      assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsOnly(add(123L)));
    }
  }

  @Test
  public void getMultipleAddEvents() throws Exception {
    try (Dataset<Long> personDataset = createPersonDataset()) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      addressAccess.add("def");
      personAccess.add(123L);
      personAccess.add(456L);
      addressAccess.add("qwe");
      personAccess.add(789L);
      addressAccess.add("asd");
      personAccess.add(0L);

        /* Ordering of events is same as that of changes only for single stripe */
      if (clusterUri.getScheme() == SS_SCHEME) {
        assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc"), add("def"), add("qwe"), add("asd")));
        assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsOnly(add(123L), add(456L), add(789L), add(0L)));
      } else {
        assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsInAnyOrder(add("abc"), add("def"), add("qwe"), add("asd")));
        assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsInAnyOrder(add(123L), add(456L), add(789L), add(0L)));
      }
    }
  }

  @Test
  public void getDeleteEvent() throws Exception {
    assertThat(datasetManager.newDataset("person", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> personDataset = datasetManager.getDataset("person", LONG)) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      addressAccess.add("def");
      addressAccess.delete("abc");
      personAccess.add(123L);
      personAccess.add(456L);
      personAccess.delete(123L);

        /* Ordering of events is same as that of changes only for single stripe */
      if (clusterUri.getScheme() == SS_SCHEME) {
        assertThatWithin(1, SECONDS, addressListener::receivedEvents, containsOnly(add("abc"), add("def"), delete("abc")));
        assertThatWithin(1, SECONDS, personListener::receivedEvents, containsOnly(add(123L), add(456L), delete(123L)));
      } else {
        assertThatWithin(1, SECONDS, addressListener::receivedEvents, containsInAnyOrder(add("abc"), add("def"), delete("abc")));
        assertThatWithin(1, SECONDS, personListener::receivedEvents, containsInAnyOrder(add(123L), add(456L), delete(123L)));
      }
    }
  }

  @Test
  public void noDeleteEventIfRecordDidNotExist() throws Exception {
    try (Dataset<Long> personDataset = createPersonDataset()) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.delete("def");
      addressAccess.add("abc");
      personAccess.delete(456L);
      personAccess.add(123L);

      assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc")));
      assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsOnly(add(123L)));
    }
  }

  @Test
  public void noAddEventIfRecordAlreadyExisted() throws Exception {
    try (Dataset<Long> personDataset = createPersonDataset()) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      addressAccess.add("abc");
      personAccess.add(123L);
      personAccess.add(123L);

      assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc")));
      assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsOnly(add(123L)));
    }
  }

  @Test
  public void deregisteringTheListenerStopsTheEvents() throws Exception {
    try (Dataset<Long> personDataset = createPersonDataset()) {

      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      personAccess.add(123L);
      addressAccess.deregisterChangeListener(addressListener);
      personAccess.deregisterChangeListener(personListener);
      addressAccess.add("def");
      personAccess.add(456L);
      addressAccess.registerChangeListener(addressListener);
      personAccess.registerChangeListener(personListener);
      addressAccess.add("qwe");
      personAccess.add(789L);

      assertThatWithin(1, TimeUnit.SECONDS, addressListener::receivedEvents, containsOnly(add("abc"), add("qwe")));
      assertThatWithin(1, TimeUnit.SECONDS, personListener::receivedEvents, containsOnly(add(123L), add(789L)));
    }
  }

  @Test
  public void keyLevelEventOrdering() throws Exception {
    assertThat(datasetManager.newDataset("person", LONG, datasetManager.datasetConfiguration()
        .offheap("offheap")
        .build()), is(true));
    try (Dataset<Long> personDataset = datasetManager.getDataset("person", LONG)) {
      DatasetWriterReader<String> addressAccess = dataset.writerReader();
      DatasetWriterReader<Long> personAccess = personDataset.writerReader();

      TestChangeListener<String> addressListener = new TestChangeListener<>();
      addressAccess.registerChangeListener(addressListener);
      TestChangeListener<Long> personListener = new TestChangeListener<>();
      personAccess.registerChangeListener(personListener);

      addressAccess.add("abc");
      addressAccess.add("def");
      addressAccess.update("def", write("intCell", 100));
      addressAccess.update("abc", write("stringCell", "stringValue"));
      addressAccess.delete("abc");
      addressAccess.update("def", write("doubleCell", 800D));

      personAccess.add(123L);
      personAccess.add(456L);
      personAccess.update(456L, write("booleanCell", true));
      personAccess.update(456L, write("booleanCell", false));
      personAccess.delete(123L);
      personAccess.delete(456L);

      assertThatWithin(1, SECONDS, "abc", key -> addressListener.receivedEvents(key), containsOnly(add("abc"), update("abc"), delete("abc")));
      assertThatWithin(1, SECONDS, "def", key -> addressListener.receivedEvents(key), containsOnly(add("def"), update("def"), update("def")));
      assertThatWithin(1, SECONDS, 123L, key -> personListener.receivedEvents(key), containsOnly(add(123L), delete(123L)));
      assertThatWithin(1, SECONDS, 456L, key -> personListener.receivedEvents(key), containsOnly(add(456L), update(456L), update(456L), delete(456L)));
    }
  }

  private static <T> void assertThatWithin(long timeout, TimeUnit unit, Supplier<T> supplier, Matcher<? super T> matcher) {
    long terminus = System.nanoTime() + unit.toNanos(timeout);

    boolean interrupted = false;
    try {
      AssertionError failure;
      long sleep = 1L;
      do {
        try {
          assertThat(supplier.get(), matcher);
          return;
        } catch (AssertionError ae) {
          failure = ae;
        }
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      } while (System.nanoTime() < terminus);
      throw failure;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private Dataset<Long> createPersonDataset() throws com.terracottatech.store.StoreException {
    assertThat(datasetManager.newDataset("person", Type.LONG, datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));
    return datasetManager.getDataset("person", Type.LONG);
  }

  private static <K extends Comparable<K>, T> void assertThatWithin(long timeout, TimeUnit unit, K key, Function<K, T> function, Matcher<? super T> matcher) {
    assertThatWithin(timeout, unit, () -> function.apply(key), matcher);
  }

  private Matcher<Iterable<? extends TestChangeListener.ChangeEvent<?>>> containsInAnyOrder(TestChangeListener.ChangeEvent<?>... events) {
    return Matchers.containsInAnyOrder(events);
  }

  private static Matcher<Iterable<? extends TestChangeListener.ChangeEvent<?>>> containsOnly(TestChangeListener.ChangeEvent<?>... items) {
    return contains(items);
  }
}
