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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This is an optimized long keyed object cache. Faster than auto boxing.
 *
 * @param <V>  the type parameter
 * @author cschanck
 */
public class SimpleLongObjectCache<V> {

  private final Entry<V>[] table;
  private final int tableSize;
  private final int maxSize;
  private int currentSize = 0;
  private Entry<V> accessListHead = null;
  private Entry<V> accessListTail = null;

  /**
   * Instantiates a new Simple long object cache.
   *
   * @param maxSize the max size
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public SimpleLongObjectCache(int maxSize) {
    int tmp = (int) (maxSize * 1.33f);
    int capacity = 1;
    while (capacity < tmp) {
      capacity <<= 1;
    }
    this.tableSize = capacity;
    this.maxSize = maxSize;
    table = new Entry[tableSize];
  }

  private int hash(long key) {
    return (int) (key);
  }

  public void cache(long key, V value) {
    int h = hash(key) & (tableSize - 1);
    if (table[h] != null) {
      for (Entry<V> ent = table[h]; ent != null; ent = ent.getNextInHashChain()) {
        if (ent.getKey() == key) {
          moveToFrontOfAccessList(ent);
          ent.setValue(value);
          return;
        }
      }
    }
    Entry<V> ent = new Entry<V>(key, value, table[h]);
    table[h] = ent;
    addToAccessList(ent);
    currentSize++;
    evictAsNeeded();
  }

  private void evictAsNeeded() {
    while (currentSize > maxSize) {
      if (accessListTail != null) {
        uncache(accessListTail.getKey());
      }
    }
  }

  private void addToAccessList(Entry<V> ent) {
    ent.setNextEntry(accessListHead);
    ent.setPriorEntry(null);
    if (accessListHead != null) {
      accessListHead.setPriorEntry(ent);
    }
    if (accessListHead == null) {
      accessListTail = ent;
    }
    accessListHead = ent;
  }

  private void moveToFrontOfAccessList(Entry<V> ent) {
    removeFromAccessList(ent);
    addToAccessList(ent);
  }

  private void removeFromAccessList(Entry<V> ent) {

    if (ent.getNextEntry() == null) {
      accessListTail = ent.getPriorEntry();
    }

    if (ent.getPriorEntry() == null) {
      accessListHead = ent.getNextEntry();
    }

    if (ent.getPriorEntry() != null) {
      ent.getPriorEntry().setNextEntry(ent.getNextEntry());
    }

    if (ent.getNextEntry() != null) {
      ent.getNextEntry().setPriorEntry(ent.getPriorEntry());
    }

  }

  public V get(long key) {
    int h = hash(key) & (tableSize - 1);
    for (Entry<V> ent = table[h]; ent != null; ent = ent.getNextInHashChain()) {
      if (ent.getKey() == key) {
        moveToFrontOfAccessList(ent);
        return ent.getValue();
      }
    }
    return null;
  }

  public void uncache(long key) {
    int h = hash(key) & (tableSize - 1);
    if (table[h] != null) {
      Entry<V> prior = null;
      for (Entry<V> ent = table[h]; ent != null; ent = ent.getNextInHashChain()) {
        if (ent.getKey() == key) {

          if (prior == null) {
            table[h] = ent.getNextInHashChain();
          } else {
            prior.setNextInHashChain(ent.getNextInHashChain());
          }

          removeFromAccessList(ent);
          currentSize--;
          break;
        }
        prior = ent;
      }
    }
  }

  public int size() {
    return currentSize;
  }

  public void clear() {
    if (currentSize > 0) {
      for (int i = 0; i < table.length; i++) {
        table[i] = null;
      }
      currentSize = 0;
      accessListTail = null;
      accessListHead = null;
    }
  }

  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println("Current size: " + currentSize);
    pw.println("List:");
    for (Entry<V> ent = accessListHead; ent != null; ent = ent.getNextEntry()) {
      pw.println("\t " + ent);
    }
    pw.println("Table:");
    for (int i = 0; i < tableSize; i++) {
      if (table[i] != null) {
        pw.print("\t" + i + ":");
        for (Entry<V> ent = table[i]; ent != null; ent = ent.getNextInHashChain()) {
          pw.print(ent + " ");
        }
      }
      pw.println();
    }

    pw.flush();
    return sw.toString();
  }

  private class Entry<VV> {
    private Entry<VV> nextInHashChain;
    private VV value;
    private final long key;
    private Entry<VV> nextEntry;
    private Entry<VV> priorEntry;

    /**
     * Instantiates a new Entry.
     *
     * @param key the key
     * @param value the value
     * @param next the next
     */
    Entry(long key, VV value, Entry<VV> next) {
      this.key = key;
      this.value = value;
      this.nextInHashChain = next;
    }

    /**
     * Gets next in hash chain.
     *
     * @return the next in hash chain
     */
    public Entry<VV> getNextInHashChain() {
      return nextInHashChain;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public VV getValue() {
      return value;
    }

    /**
     * Gets key.
     *
     * @return the key
     */
    public long getKey() {
      return key;
    }

    /**
     * Sets next in hash chain.
     *
     * @param nextInHashChain the next in hash chain
     */
    public void setNextInHashChain(Entry<VV> nextInHashChain) {
      this.nextInHashChain = nextInHashChain;
    }

    /**
     * Gets next entry.
     *
     * @return the next entry
     */
    public Entry<VV> getNextEntry() {
      return nextEntry;
    }

    /**
     * Gets prior entry.
     *
     * @return the prior entry
     */
    public Entry<VV> getPriorEntry() {
      return priorEntry;
    }

    /**
     * Sets next entry.
     *
     * @param nextEntry the next entry
     */
    public void setNextEntry(Entry<VV> nextEntry) {
      this.nextEntry = nextEntry;
    }

    /**
     * Sets prior entry.
     *
     * @param priorEntry the prior entry
     */
    public void setPriorEntry(Entry<VV> priorEntry) {
      this.priorEntry = priorEntry;
    }

    @Override
    public String toString() {
      return "Entry{" +
        "value=" + value +
        ", key=" + key +
        '}';
    }

    /**
     * Sets value.
     *
     * @param value the value
     */
    public void setValue(VV value) {
      this.value = value;
    }
  }

}
