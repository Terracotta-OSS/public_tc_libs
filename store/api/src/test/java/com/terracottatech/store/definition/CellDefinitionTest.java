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

package com.terracottatech.store.definition;

import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.INT;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class CellDefinitionTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testNullPointerExceptionOnNullName() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Definition name must be non-null");
    define(null, Type.BOOL);
  }

  @Test
  public void testNullPointerExceptionOnNullType() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Definition type must be non-null");
    define("foo", null);
  }

  @Test
  public void testEqualToSelf() {
    CellDefinition<Boolean> defn = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    assertTrue(defn.equals(defn));
  }

  @Test
  public void testNotEqualToNull() {
    CellDefinition<Boolean> defn = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    assertFalse(defn.equals(null));
  }

  @Test
  public void testEqualDefinitions() {
    CellDefinition<Boolean> a = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    CellDefinition<Boolean> b = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    assertTrue(a.equals(b));
    assertTrue(b.equals(a));
  }

  @Test
  public void testEqualDefinitionsAreInterned() {
    CellDefinition<Boolean> a = define("foo", BOOL);
    CellDefinition<Boolean> b = define("foo", BOOL);
    assertThat(a, sameInstance(b));
  }

  @Test
  public void testDefinitionsNotEqualOnName() {
    CellDefinition<Boolean> a = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    CellDefinition<Boolean> b = new CellDefinition.Impl<Boolean>("bar", BOOL) {};
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test
  public void testDefinitionsNotEqualOnType() {
    CellDefinition<Boolean> a = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    CellDefinition<Integer> b = new CellDefinition.Impl<Integer>("foo", INT) {};
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
  }

  @Test
  public void testEqualDefinitionsHaveEqualHashcodes() {
    CellDefinition<Boolean> a = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    CellDefinition<Boolean> b = new CellDefinition.Impl<Boolean>("foo", BOOL) {};
    assertThat(a.hashCode(), is(b.hashCode()));
  }

  @Test
  public void testNullPointerExceptionOnNullValue() {
    CellDefinition<Boolean> a = new CellDefinition.Impl<Boolean>("foo", BOOL) {};

    exception.expect(NullPointerException.class);
    exception.expectMessage("Cell value must be non-null");
    a.newCell(null);
  }
}
