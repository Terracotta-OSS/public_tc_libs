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

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;

import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TestChangeListener<T extends Comparable<T>> implements ChangeListener<T> {

  public static class ChangeEvent<T extends Comparable<T>> {
    private final T key;
    private final ChangeType type;

    private ChangeEvent(T key, ChangeType type) {
      this.key = requireNonNull(key);
      this.type = requireNonNull(type);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ChangeEvent<?>) {
        return key.equals(((ChangeEvent) o).key) && type.equals(((ChangeEvent) o).type);
      } else {
        return false;
      }
    }

    public T getKey() {
      return key;
    }

    @Override
    public int hashCode() {
      return key.hashCode() ^ type.hashCode();
    }

    @Override
    public String toString() {
      return "Event[" + key + ": " + type + "]";
    }
  }

  public static <T extends Comparable<T>> ChangeEvent<T> add(T key) {
    return new ChangeEvent<T>(key, ChangeType.ADDITION);
  }

  public static <T extends Comparable<T>> ChangeEvent<T> update(T key) {
    return new ChangeEvent<T>(key, ChangeType.MUTATION);
  }

  public static <T extends Comparable<T>> ChangeEvent<T> delete(T key) {
    return new ChangeEvent<T>(key, ChangeType.DELETION);
  }

  private final CopyOnWriteArrayList<ChangeEvent<?>> events = new CopyOnWriteArrayList<>();
  private volatile boolean missedEvents = false;

  @Override
  public void onChange(T key, ChangeType changeType) {
    events.add(new ChangeEvent<>(key, changeType));
  }

  @Override
  public void missedEvents() {
    this.missedEvents = true;
  }

  public Iterable<ChangeEvent<?>> receivedEvents() {
    if (missedEvents) {
      throw new AssertionError("Events were lost! We only know: " + events);
    } else {
      return Collections.unmodifiableList(events);
    }
  }

  public Iterable<ChangeEvent<?>> receivedEvents(T key) {
    if (missedEvents) {
      throw new AssertionError("Events were lost! We only know: " + events);
    } else {
      return Collections.unmodifiableList(events.stream()
                                                .filter(event -> event.getKey().equals(key))
                                                .collect(Collectors.toList()));
    }
  }
}
