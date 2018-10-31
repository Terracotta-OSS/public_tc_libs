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
package com.terracottatech.store;

import org.junit.Test;

import java.util.Date;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class CellTest {

  @Test
  public void testIllegalArgumentExceptionOnIllegalType() {
    try {
      Cell.cell("foo", new Date());
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("No support for type: class java.util.Date"));
    }
  }

  @Test
  public void testNullPointerExceptionOnNullValue() {
    try {
      Cell.cell("foo", null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e.getMessage(), is("Cell value must be non-null"));
    }
  }

  @Test
  public void testEqualToSelf() {
    Cell<Integer> cell = Cell.cell("foo", 12);
    assertThat(cell, equalTo(cell));
  }

  @Test
  public void testNotEqualToNull() {
    Cell<Integer> cell = Cell.cell("foo", 12);
    assertFalse(cell.equals(null));
  }

  @Test
  public void testCellEqual() {
    Cell<Boolean> a = Cell.cell("foo", true);
    Cell<Boolean> b = Cell.cell("foo", true);
    assertTrue(a.equals(b));
  }

  @Test
  public void testCellNotEqualOnValue() {
    Cell<Boolean> a = Cell.cell("foo", true);
    Cell<Boolean> b = Cell.cell("foo", false);
    assertFalse(a.equals(b));
  }

  @Test
  public void testEqualCellsHaveEqualHashcodes() {
    Cell<Boolean> a = Cell.cell("foo", true);
    Cell<Boolean> b = Cell.cell("foo", true);
    assertThat(a.hashCode(), is(b.hashCode()));
  }
}
