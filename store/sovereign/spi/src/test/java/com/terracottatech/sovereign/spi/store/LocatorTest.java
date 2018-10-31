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
package com.terracottatech.sovereign.spi.store;

import static com.terracottatech.sovereign.spi.store.Locator.TraversalDirection.FORWARD;
import static com.terracottatech.sovereign.spi.store.Locator.TraversalDirection.REVERSE;
import java.util.NoSuchElementException;
import static org.hamcrest.CoreMatchers.equalTo;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author mscott
 */
public class LocatorTest {

  public LocatorTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {

  }

  @After
  public void tearDown() {
  }

  /**
   * Test of iterator method, of class Locator.
   */
  @Test
  public void testIterator() {
    System.out.println("iterator");
    Locator instance = new MockLocator(0, 10, FORWARD);
    int count = 0;
    for (Locator l : instance) {
      count += 1;
      assertTrue(l.isValid());
      assertTrue(!l.isEndpoint());
    }
    assertThat("number of valid locators", count, equalTo(10));
    instance = new MockLocator(10, 0, REVERSE);
    count = 0;
    for (Locator l : instance) {
      count += 1;
      assertTrue(l.isValid());
      assertTrue(!l.isEndpoint());
    }
    assertThat("number of valid locators", count, equalTo(10));
  }

  class MockLocator implements Locator {

    private final int count;
    private final int limit;
    private final TraversalDirection dir;

    public MockLocator(int count, int limit, TraversalDirection dir) {
      this.count = count;
      this.limit = limit;
      this.dir = dir;
    }

    @Override
    public Locator next() {
      if (limit == count) {
        throw new NoSuchElementException();
      }
      return (dir == FORWARD) ? new MockLocator(count + 1, limit, dir) : (dir == REVERSE) ? new MockLocator(count - 1, limit, dir) : new MockLocator(count, limit, dir);
    }

    @Override
    public boolean isEndpoint() {
      return limit == count;
    }

    @Override
    public TraversalDirection direction() {
      return dir;
    }

  }

}
