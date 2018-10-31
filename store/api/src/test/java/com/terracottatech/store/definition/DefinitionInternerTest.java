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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DefinitionInterner;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.LONG;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 *
 * @author cdennis
 */
public class DefinitionInternerTest {

  @Test
  public void testSingleIntern() {
    DefinitionInterner interner = new DefinitionInterner(CellDefinitionImpl::new);

    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    CellDefinition<Boolean> b = interner.intern("foo", BOOL);

    assertThat(a, sameInstance(b));
  }

  @Test
  public void testNameCollision() {
    DefinitionInterner interner = new DefinitionInterner(CellDefinitionImpl::new);

    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    CellDefinition<Integer> b = interner.intern("foo", INT);

    assertThat(a.type(), Is.is(Type.BOOL));
    assertThat(b.type(), is(Type.INT));

    assertThat(interner.intern("foo", BOOL), sameInstance(a));
    assertThat(interner.intern("foo", INT), sameInstance(b));
  }

  @Test
  public void testTripleNameCollision() {
    DefinitionInterner interner = new DefinitionInterner(CellDefinitionImpl::new);

    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    CellDefinition<Integer> b = interner.intern("foo", INT);
    CellDefinition<Long> c = interner.intern("foo", LONG);

    assertThat(a.type(), is(BOOL));
    assertThat(b.type(), is(INT));
    assertThat(c.type(), is(LONG));

    assertThat(interner.intern("foo", BOOL), sameInstance(a));
    assertThat(interner.intern("foo", INT), sameInstance(b));
    assertThat(interner.intern("foo", LONG), sameInstance(c));
  }

  @Test
  public void testDifferentNames() {
    DefinitionInterner interner = new DefinitionInterner(CellDefinitionImpl::new);

    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    CellDefinition<Boolean> b = interner.intern("bar", BOOL);

    assertThat(a.name(), is("foo"));
    assertThat(b.name(), is("bar"));

    assertThat(interner.intern("foo", BOOL), sameInstance(a));
    assertThat(interner.intern("bar", BOOL), sameInstance(b));
  }

  @Test
  public void testSimpleGarbageCollection() throws InterruptedException {
    AtomicInteger generatorCount = new AtomicInteger();

    BiFunction<String, Type<?>, CellDefinition<?>> generator = (name, type) -> {
      generatorCount.incrementAndGet();
      return new CellDefinitionImpl<>(name, type);
    };

    DefinitionInterner interner = new DefinitionInterner(generator);
    {
      interner.intern("foo", BOOL);
    }
    assertThat(generatorCount.intValue(), is(1));

    System.gc();
    Thread.sleep(1000);
    interner.intern("foo", BOOL);
    assertThat(generatorCount.intValue(), is(2));
  }

  @Test
  public void testMultiToSingleGarbageCollection() throws InterruptedException {
    AtomicInteger generatorCount = new AtomicInteger();

    BiFunction<String, Type<?>, CellDefinition<?>> generator = (name, type) -> {
      generatorCount.incrementAndGet();
      return new CellDefinitionImpl<>(name, type);
    };

    DefinitionInterner interner = new DefinitionInterner(generator);
    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    {
      interner.intern("foo", INT);
    }
    assertThat(generatorCount.intValue(), is(2));

    System.gc();
    Thread.sleep(1000);
    interner.intern("foo", INT);
    assertThat(generatorCount.intValue(), is(3));
    assertThat(interner.intern("foo", BOOL), sameInstance(a));
  }

  @Test
  public void testMultiToMultiGarbageCollection() throws InterruptedException {
    AtomicInteger generatorCount = new AtomicInteger();

    BiFunction<String, Type<?>, CellDefinition<?>> generator = (name, type) -> {
      generatorCount.incrementAndGet();
      return new CellDefinitionImpl<>(name, type);
    };

    DefinitionInterner interner = new DefinitionInterner(generator);
    CellDefinition<Boolean> a = interner.intern("foo", BOOL);
    CellDefinition<Integer> b = interner.intern("foo", INT);
    {
      interner.intern("foo", LONG);
    }
    assertThat(generatorCount.intValue(), is(3));

    System.gc();
    Thread.sleep(1000);
    interner.intern("foo", LONG);
    assertThat(generatorCount.intValue(), is(4));
    assertThat(interner.intern("foo", BOOL), sameInstance(a));
    assertThat(interner.intern("foo", INT), sameInstance(b));
  }

  @Test
  public void testClassGC() throws ClassNotFoundException, ReflectiveOperationException, InterruptedException {
    WeakReference<Class<?>> klazzRef = new WeakReference<>(doOtherClassLoaderStuff());
    while (klazzRef.get() != null) {
      System.gc();
      Thread.sleep(100);
    }
  }

  private static Class<?> doOtherClassLoaderStuff() throws ClassNotFoundException, ReflectiveOperationException {
    ClassLoader current = DefinitionInterner.class.getClassLoader();
    assumeThat(current, instanceOf(URLClassLoader.class));

    URL[] currentUrls = ((URLClassLoader) current).getURLs();

    URLClassLoader isolatedLoader = new URLClassLoader(currentUrls, current.getParent());
    Class<?> definitionInternerKlazz = isolatedLoader.loadClass(DefinitionInterner.class.getName());
    Class<?> cellDefinitionImplKlazz = isolatedLoader.loadClass(CellDefinitionImpl.class.getName());
    Class<?> typeKlazz = isolatedLoader.loadClass(Type.class.getName());

    Constructor<?> internerConstructor = definitionInternerKlazz.getDeclaredConstructor(BiFunction.class);
    internerConstructor.setAccessible(true);
    Constructor<?> definitionConstructor = cellDefinitionImplKlazz.getConstructor(String.class, typeKlazz);
    definitionConstructor.setAccessible(true);
    Object interner = internerConstructor.newInstance(new BiFunction<Object, Object, Object>() {
      @Override
      public Object apply(Object t, Object u) {
        try {
          return definitionConstructor.newInstance(t, u);
        } catch (ReflectiveOperationException ex) {
          throw new AssertionError(ex);
        }
      }
    });

    Method internMethod = definitionInternerKlazz.getMethod("intern", String.class, typeKlazz);
    internMethod.setAccessible(true);
    assertThat(internMethod.invoke(interner, "foo", typeKlazz.getField("INT").get(null)), notNullValue());
    return definitionInternerKlazz;
  }

  static class CellDefinitionImpl<T> implements CellDefinition<T> {

    private final String name;
    private final Type<T> type;

    public CellDefinitionImpl(String name, Type<T> type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type<T> type() {
      return type;
    }

    @Override
    public Cell<T> newCell(T value) {
      throw new UnsupportedOperationException();
    }
  }
}
