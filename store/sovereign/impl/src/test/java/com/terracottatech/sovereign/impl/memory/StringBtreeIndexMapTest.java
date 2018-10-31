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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.spi.store.Locator;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 *
 * @author mscott
 */
public class StringBtreeIndexMapTest extends BtreeIndexMapTest<String> {

  public StringBtreeIndexMapTest() {
  }

  @Override
  public Class<String> keyType() {
    return String.class;
  }

  @Test
  public void testAdd() {
    super.add(new String[]{"Tom", "Dick", "Harry"});
  }

    /**
   * Test of get method, of class BtreeIndexMap.
   */
  @Test
  public void teststruct() throws Exception {
    ArrayList<String> cap = new ArrayList<>();
    InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("sample.txt"));
    LineNumberReader liner = new LineNumberReader(reader);
    String line = liner.readLine();
    Pattern p = Pattern.compile("([\\w]+)\\b");
    while (line != null) {
      java.util.regex.Matcher m = p.matcher(line);
      while (m.find()) {
        String name = m.group();
        cap.add(name);
        map.put(cap.size(), context, name, new PersistentMemoryLocator(cap.size(), null));
      }
      line = liner.readLine();
    }
    String last = "0";
    Locator loc = map.first(context);
    while (!loc.isEndpoint()) {
      String next = cap.get((int) (((PersistentMemoryLocator)loc).index() - 1));
      Assert.assertTrue(next.compareTo(last) >= 0);
      last = next;
      loc = loc.next();
    }
  }

  @Test
  public void testdups() throws Exception {
    super.internalTestDups(toArray("a a a a a a a a a a a a a a a a a a a a b"));
  }

  @Test
  public void testmiddle() throws Exception {
    super.middlecount(toArray("a a a a a a a a a a b c A A B B"), "a");
  }

  @Test
  public void testhigher() throws Exception {
//  test in the middle of dups
    super.higher(toArray("a a a a a a a a a a b c A A B B"), "a", 2);
//  test past the end
    super.higher(toArray("a a a a a a a a a a b c A A B B"), String.valueOf(Character.MAX_VALUE), 0);
//  test past the beginning
    super.higher(toArray("a a a a a a a a a a b c A A B B"), String.valueOf(Character.MIN_VALUE), 16);
  }

  @Test
  public void testlower() throws Exception {
    super.lower(toArray("a a a a a a a a a a b c A A B B"), "a", 4);
//  test past the end
    super.lower(toArray("a a a a a a a a a a b c A A B B"), String.valueOf(Character.MIN_VALUE), 0);
//  test past the beginning
    super.lower(toArray("a a a a a a a a a a b c A A B B"), String.valueOf(Character.MAX_VALUE), 16);
  }

  @Test
  public void testProperDeletion()  throws Exception {
    super.testDeletion(toArray("a a a a a a a a a a b c A A B B"));
  }

  private String[] toArray(String line) {
    Pattern p = Pattern.compile("([\\w]+)\\b");
    java.util.regex.Matcher m = p.matcher(line);
    ArrayList<String> words = new ArrayList<>();
    while (m.find()) {
      String next = m.group();
      words.add(next);
    }
    return words.toArray(new String[words.size()]);
  }

  @Test
  public void testBatch() throws Exception {

    Supplier<String> sup=new Supplier<String>() {
      Random r=new Random();
      @Override
      public String get() {
        return "k"+r.nextInt();
      }
    };
    testBatchIngestion(100000, sup);
  }
}
