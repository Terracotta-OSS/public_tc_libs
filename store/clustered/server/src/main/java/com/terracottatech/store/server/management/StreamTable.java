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
package com.terracottatech.store.server.management;

import com.terracottatech.store.server.stream.PipelineProcessor;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Math.min;
import static java.util.Comparator.comparing;

public class StreamTable {

  private final ConcurrentMap<StreamShape, StreamStatistics> map = new ConcurrentHashMap<>();
  private final Deque<Map.Entry<StreamShape, StreamStatistics>> newStatistics = new ConcurrentLinkedDeque<>();
  private final List<Map.Entry<StreamShape, StreamStatistics>> sortedStatistics = new ArrayList<>();

  private final Comparator<Map.Entry<?, StreamStatistics>> comparator;
  private final Duration window;
  private final float accuracy;

  public StreamTable(Duration window, float accuracy, Comparator<StreamStatistics> comparator) {
    this.comparator = comparing(Map.Entry::getValue, comparator);
    this.window = window;
    this.accuracy = accuracy;
  }

  public void increment(PipelineProcessor stream) {
    StreamShape shape = stream.getStreamShape();
    while (true) {
      StreamStatistics stats = map.get(shape);
      if (stats == null) {
        stats = new StreamStatistics(window, accuracy, stream);
        StreamStatistics race = map.putIfAbsent(shape, stats);
        if (race == null) {
          newStatistics.add(new AbstractMap.SimpleImmutableEntry<>(shape, stats));
          expireQueue();
          return;
        } else {
          stats = race;
        }
      }

      try {
        stats.increment(stream);
        return;
      } catch (IllegalStateException e) {
        map.remove(shape, stats);
      }
    }
  }

  private void expireQueue() {
    Map.Entry<StreamShape, StreamStatistics> peek = newStatistics.peek();
    if (peek.getValue().tryExpire()) {
      Map.Entry<StreamShape, StreamStatistics> removed = newStatistics.poll();
      if (removed == peek) {
        map.remove(removed.getKey(), removed.getValue());
      } else {
        newStatistics.push(removed);
      }
    }
  }

  public synchronized List<Map.Entry<StreamShape, StreamStatistics>> top(int count) {
    Map.Entry<StreamShape, StreamStatistics> entry;
    while ((entry = newStatistics.poll()) != null) {
      sortedStatistics.add(entry);
    }
    sortedStatistics.sort(comparator.reversed());
    for (ListIterator<Map.Entry<StreamShape, StreamStatistics>> it = sortedStatistics.listIterator(sortedStatistics.size()); it.hasPrevious(); ) {
      Map.Entry<StreamShape, StreamStatistics> last = it.previous();
      if (last.getValue().tryExpire()) {
        it.remove();
        map.remove(last.getKey(), last.getValue());
      }
    }
    return sortedStatistics.subList(0, min(count, sortedStatistics.size()));
  }
}
