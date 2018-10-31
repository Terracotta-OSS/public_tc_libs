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
package com.terracottatech.store.client;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.SuccessResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeListenerRegistryTest {
  private ChangeListenerRegistry<String> registry;
  private ArgumentCaptor<SendChangeEventsMessage> messagesSentCaptor;

  @Before
  public void before() {
    MessageSender messageSender = mock(MessageSender.class);
    messagesSentCaptor = ArgumentCaptor.forClass(SendChangeEventsMessage.class);
    when(messageSender.sendMessageAwaitResponse(messagesSentCaptor.capture(), eq(SendConfiguration.ONE_SERVER))).thenReturn(new SuccessResponse());

    registry = new ChangeListenerRegistry<>(messageSender);
  }

  @Test
  public void noListenersNoMessages() {
    verifyMessages();
  }

  @Test
  public void sendMessageOnFirstListener() {
    registry.registerChangeListener(new MyChangeListener("1", 0));
    verifyMessages(true);
  }

  @Test
  public void reregisteringListenerDoesNotSendAnotherMessage() {
    ChangeListener<String> listener = new MyChangeListener("1", 0);
    registry.registerChangeListener(listener);
    registry.registerChangeListener(listener);
    verifyMessages(true);
  }

  @Test
  public void registeringASecondListenerDoesNotSendAnotherMessage() {
    registry.registerChangeListener(new MyChangeListener("1", 0));
    registry.registerChangeListener(new MyChangeListener("2", 0));
    verifyMessages(true);
  }

  @Test
  public void deregisteringSendsAnOffMessage() {
    ChangeListener<String> listener = new MyChangeListener("1", 0);
    registry.registerChangeListener(listener);
    registry.deregisterChangeListener(listener);
    verifyMessages(true, false);
  }

  @Test
  public void noOffMessagesIfOtherListenersStillRegistered() {
    ChangeListener<String> listener = new MyChangeListener("1", 0);
    registry.registerChangeListener(listener);
    registry.registerChangeListener(new MyChangeListener("2", 0));
    registry.deregisterChangeListener(listener);
    verifyMessages(true);
  }

  @Test
  public void ifMultipleListenersAreRegisteredThenDeregisteringAllSendsAnOffMessage() {
    ChangeListener<String> listener1 = new MyChangeListener("1", 0);
    ChangeListener<String> listener2 = new MyChangeListener("2", 0);
    registry.registerChangeListener(listener1);
    registry.registerChangeListener(listener2);
    registry.deregisterChangeListener(listener1);
    registry.deregisterChangeListener(listener2);
    verifyMessages(true, false);
  }

  @Test
  public void registeringAfterAnOffMessageSendsANewOnMessage() {
    ChangeListener<String> listener1 = new MyChangeListener("1", 0);
    ChangeListener<String> listener2 = new MyChangeListener("2", 0);

    registry.registerChangeListener(listener1);
    registry.registerChangeListener(listener2);
    registry.deregisterChangeListener(listener1);
    registry.deregisterChangeListener(listener2);

    registry.registerChangeListener(new MyChangeListener("3", 0));
    verifyMessages(true, false, true);
  }

  @Test
  public void eventsAreForwardedToListener() throws Exception {
    MyChangeListener listener = new MyChangeListener("1", 1);
    registry.registerChangeListener(listener);

    registry.onChange("key", ChangeType.ADDITION);

    registry.shutdown(10000);

    List<String> events = listener.getEvents();
    assertEquals(":1:key:ADDITION:", events.get(0));

    listener.checkNotTooManyEvents();
  }

  @Test
  public void multipleEventsAreForwardedToListener() throws Exception {
    MyChangeListener listener = new MyChangeListener("1", 2);
    registry.registerChangeListener(listener);

    registry.onChange("key", ChangeType.ADDITION);
    registry.onChange("key", ChangeType.DELETION);

    registry.shutdown(10000);

    List<String> events = listener.getEvents();
    assertEquals(":1:key:ADDITION:", events.get(0));
    assertEquals(":1:key:DELETION:", events.get(1));

    listener.checkNotTooManyEvents();
  }

  @Test
  public void multipleEventsAreForwardedToMultipleListeners() throws Exception {
    MyChangeListener listener1 = new MyChangeListener("1", 2);
    MyChangeListener listener2 = new MyChangeListener("2", 2);
    registry.registerChangeListener(listener1);
    registry.registerChangeListener(listener2);

    registry.onChange("key", ChangeType.ADDITION);
    registry.onChange("key", ChangeType.DELETION);

    registry.shutdown(10000);

    List<String> events1 = listener1.getEvents();
    List<String> events2 = listener2.getEvents();

    assertEquals(":1:key:ADDITION:", events1.get(0));
    assertEquals(":1:key:DELETION:", events1.get(1));

    assertEquals(":2:key:ADDITION:", events2.get(0));
    assertEquals(":2:key:DELETION:", events2.get(1));

    listener1.checkNotTooManyEvents();
    listener2.checkNotTooManyEvents();
  }

  @Test
  public void afterDeregisteringListenersDoNotReceiveEvents() throws Exception {
    MyChangeListener listener1 = new MyChangeListener("1", 2);
    MyChangeListener listener2 = new MyChangeListener("2", 1);
    registry.registerChangeListener(listener1);
    registry.registerChangeListener(listener2);

    registry.onChange("key1", ChangeType.ADDITION);
    registry.deregisterChangeListener(listener2);

    registry.onChange("key2", ChangeType.DELETION);

    registry.deregisterChangeListener(listener1);
    registry.onChange("key3", ChangeType.ADDITION);

    registry.shutdown(10000);

    List<String> events1 = listener1.getEvents();
    List<String> events2 = listener2.getEvents();

    assertFalse(events1.contains(":1:key3:ADDITION:"));
    assertFalse(events2.contains(":2:key2:DELETION:"));
    assertFalse(events2.contains(":2:key3:ADDITION:"));

    listener1.checkNotTooManyEvents();
    listener2.checkNotTooManyEvents();
  }

  @Test
  public void eventsAreOrdered() throws Exception {
    MyChangeListener listener = new MyChangeListener("1", 10_000);
    registry.registerChangeListener(listener);

    for (int i = 100_000; i < 110_000; i++) {
      registry.onChange("key" + i, ChangeType.ADDITION);
    }

    registry.shutdown(10000);

    List<String> events = listener.getEvents();

    List<String> sortedEvents = new ArrayList<>(events);
    Collections.sort(sortedEvents);

    assertEquals(events, sortedEvents);

    listener.checkNotTooManyEvents();
  }

  @Test
  public void slowListenersWillGetMissedEvents() throws Exception {
    MyChangeListener listener = new MyChangeListener("1", 100_000, 10);
    registry.registerChangeListener(listener);

    for (int i = 100_000; i < 200_000; i++) {
      registry.onChange("key" + i, ChangeType.ADDITION);
    }

    assertTrue(listener.waitForMissedEvents(5000));
  }

  private void verifyMessages(Boolean... expectedValues) {
    List<SendChangeEventsMessage> messagesSent = messagesSentCaptor.getAllValues();

    assertEquals(expectedValues.length, messagesSent.size());

    for (int i = 0; i < expectedValues.length; i++) {
      SendChangeEventsMessage message = messagesSent.get(i);
      assertEquals(expectedValues[i], message.sendChangeEvents());
    }

    if (expectedValues.length == 0) {
      assertFalse(registry.sendChangeEvents());
    } else {
      assertEquals(expectedValues[expectedValues.length - 1], registry.sendChangeEvents());
    }
  }

  private static class MyChangeListener implements ChangeListener<String> {
    private final String id;
    private final CountDownLatch latch;
    private final long delay;
    private final List<String> events = new ArrayList<>();
    private volatile boolean tooManyEvents;
    private final CountDownLatch missedEvents = new CountDownLatch(1);

    public MyChangeListener(String id, int expectedEventCount) {
      this(id, expectedEventCount, 0L);
    }

    public MyChangeListener(String id, int expectedEventCount, long delay) {
      this.id = id;
      this.latch = new CountDownLatch(expectedEventCount);
      this.delay = delay;
    }

    @Override
    public synchronized void onChange(String key, ChangeType changeType) {
      if (latch.getCount() == 0) {
        tooManyEvents = true;
      }

      events.add(":" + id + ":" + key + ":" + changeType + ":");

      latch.countDown();

      if (delay > 0) {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void missedEvents() {
      missedEvents.countDown();
    }

    private void checkNotTooManyEvents() throws Exception {
      if (tooManyEvents) {
        throw new RuntimeException("Received too many events");
      }
    }

    private boolean waitForMissedEvents(long timeoutMilliseconds) throws Exception {
      return missedEvents.await(timeoutMilliseconds, TimeUnit.MILLISECONDS);
    }

    private List<String> getEvents() throws Exception {
      return events;
    }
  }
}
