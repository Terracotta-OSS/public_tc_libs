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

package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.exceptions.InvalidConfigException;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReferenceVersionLimitStrategy;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Type;
import org.junit.Test;
import org.mockito.invocation.DescribedInvocation;
import org.mockito.invocation.Invocation;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.RecordBufferStrategyType.Simple;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * @author Clifford W. Johnson
 */
public class SovereignDataSetConfigTest {


  @Test
  public void testCtor() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class);
    assertThat(config.getType(), is(Type.INT));
    assertThat(config.getTimeReferenceGenerator(), is(nullValue()));
    assertThat(config.getVersionLimitStrategy(), is(not(nullValue())));
    assertThat(config.getStorageType(), is(SovereignDataSetConfig.StorageType.HEAP));
    assertThat(config.getStorage(), is(instanceOf(StorageTransient.class)));
    assertThat(config.getConcurrency(), is(greaterThan(0)));
    assertThat(config.getResourceSize(), is(0L));
    assertThat(config.getVersionLimit(), is(1));
    assertThat(config.isFairLocking(), is(true));
    assertThat(config.getAlias(), is(nullValue()));
    assertThat(config.getRecordLockTimeout(), is(greaterThan(0L)));
    assertThat(config.getDiskDurability(), is(SovereignDatasetDiskDurability.NONE));
    try {
      config.validate();
      fail();
    } catch (InvalidConfigException e) {
      // expected
    }
  }

  @Test
  public void testCtorNullType() throws Exception {
    try {
      new SovereignDataSetConfig<>(null, TestTimeReference.class);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCtorSupportedKeyTypes() throws Exception {
    for (final Type<?> keyType : new Type<?>[] {Type.BOOL, Type.CHAR, Type.DOUBLE, Type.INT, Type.LONG, Type.STRING}) {
      new SovereignDataSetConfig(keyType, TestTimeReference.class);
    }

    try {
      new SovereignDataSetConfig(Type.BYTES, TestTimeReference.class);
      fail();
    } catch (InvalidConfigException e) {
      // expected
    }
  }

  @Test
  public void testCtorNullTimeReferenceType() throws Exception {
    try {
      new SovereignDataSetConfig<>(Type.INT, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testRecordLockTimeout() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class);
    assertThat(config.getRecordLockTimeout(), is(greaterThan(0L)));

    assertThat(config.recordLockTimeout(15, TimeUnit.MINUTES), is(sameInstance(config)));
    assertThat(config.getRecordLockTimeout(), is(TimeUnit.MINUTES.toMillis(15L)));

    try {
      config.recordLockTimeout(-1, TimeUnit.MINUTES);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }

    try {
      config.recordLockTimeout(1, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    assertThat(config.freeze(), sameInstance(config));
    try {
      config.recordLockTimeout(1, TimeUnit.MINUTES);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }

    assertThat(config.getRecordLockTimeout(), is(TimeUnit.MINUTES.toMillis(15L)));

    final SovereignDataSetConfig<Integer, TestTimeReference> duplicate = config.duplicate();
    assertThat(duplicate, is(not(sameInstance(config))));
    assertThat(duplicate.getRecordLockTimeout(), is(config.getRecordLockTimeout()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testVersionLimitStrategy() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class);
    final VersionLimitStrategy defaultStrategy = config.getVersionLimitStrategy();
    assertThat(defaultStrategy, is(not(nullValue())));

    final TestTimeReference.Generator generator = new TestTimeReference.Generator();

    final BiFunction<TimeReference, TimeReference, VersionLimitStrategy.Retention> defaultFilter =
        defaultStrategy.getTimeReferenceFilter();
    assertThat(defaultFilter, is(not(nullValue())));
    assertThat(defaultFilter.apply(generator.get(), generator.get()), is(VersionLimitStrategy.Retention.FINISH));

    final TestTimeReference.LimitStrategy strategy = new TestTimeReference.LimitStrategy();
    assertThat(config.versionLimitStrategy(strategy), sameInstance(config));
    assertThat(config.getVersionLimitStrategy(), sameInstance(strategy));

    try {
      config.versionLimitStrategy((VersionLimitStrategy)new SystemTimeReferenceVersionLimitStrategy(0, TimeUnit.MILLISECONDS));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      config.versionLimitStrategy(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    assertThat(config.freeze(), sameInstance(config));
    try {
      config.versionLimitStrategy(new TestTimeReference.LimitStrategy());
      fail();
    } catch (IllegalStateException e) {
      // expected
    }

    final SovereignDataSetConfig<Integer, TestTimeReference> duplicate = config.duplicate();
    assertThat(duplicate, is(not(sameInstance(config))));
    assertThat(duplicate.getVersionLimitStrategy(), is(sameInstance(config.getVersionLimitStrategy())));
  }

  @Test
  public void testSystemTimeReferenceVersionLimitStrategy() throws Exception {
    final SovereignDataSetConfig<Integer, SystemTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class);

    final SystemTimeReferenceVersionLimitStrategy strategy =
        new SystemTimeReferenceVersionLimitStrategy(0, TimeUnit.MILLISECONDS);
    assertThat(config.versionLimitStrategy(strategy), sameInstance(config));
    assertThat(config.getVersionLimitStrategy(), sameInstance(strategy));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTimeReferenceGenerator() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class);
    assertThat(config.getTimeReferenceGenerator(), is(nullValue()));

    final TestTimeReference.Generator generator = new TestTimeReference.Generator();
    assertThat(config.timeReferenceGenerator(generator), sameInstance(config));
    assertThat(config.getTimeReferenceGenerator(), sameInstance(generator));

    try {
      config.timeReferenceGenerator((TimeReferenceGenerator)new SystemTimeReference.Generator());   // intentional cross-typing
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      config.timeReferenceGenerator(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    assertThat(config.freeze(), sameInstance(config));
    try {
      config.timeReferenceGenerator(new TestTimeReference.Generator());
      // expected, now
    } catch (IllegalStateException e) {
      fail();
    }

    final SovereignDataSetConfig<Integer, TestTimeReference> duplicate = config.duplicate();
    assertThat(duplicate, is(not(sameInstance(config))));
    assertThat(duplicate.getTimeReferenceGenerator(), is(sameInstance(config.getTimeReferenceGenerator())));
  }

  @Test
  public void testFixedTimeReferenceGenerator() throws Exception {
    final SovereignDataSetConfig<Integer, FixedTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, FixedTimeReference.class);
    assertThat(config.getTimeReferenceGenerator(), is(instanceOf(FixedTimeReference.Generator.class)));

    final FixedTimeReference.Generator generator = new FixedTimeReference.Generator();
    assertThat(config.timeReferenceGenerator(generator), sameInstance(config));
    assertThat(config.getTimeReferenceGenerator(), sameInstance(generator));
  }

  @Test
  public void testSystemTimeReferenceGenerator() throws Exception {
    final SovereignDataSetConfig<Integer, SystemTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class);
    assertThat(config.getTimeReferenceGenerator(), is(instanceOf(SystemTimeReference.Generator.class)));

    final SystemTimeReference.Generator generator = new SystemTimeReference.Generator();
    assertThat(config.timeReferenceGenerator(generator), sameInstance(config));
    assertThat(config.getTimeReferenceGenerator(), sameInstance(generator));
  }

  /**
   * Attempts to ensure that {@link SovereignDataSetConfig#duplicate()} copies all configuration fields.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDuplicate() throws Exception {
    final SovereignDataSetConfig<Integer, SystemTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class);

    /*
     * Local InvocationListener used to monitor calls to methods on a Mockito spy instance
     * (set below).  This listener keeps a count of calls to the list of methods provided
     * in its constructor.  For purposes of this test, the list is the collection of
     * configuration setters that must be called while setting up the conditions for the
     * test.
     */
    final class SetterListener implements InvocationListener {

      private final Map<Method, Integer> callCounters = new HashMap<>();

      SetterListener(final List<Method> trackedMethods) {
        Objects.requireNonNull(trackedMethods);
        for (final Method trackedMethod : trackedMethods) {
          this.callCounters.put(trackedMethod, 0);
        }
      }

      @Override
      public void reportInvocation(final MethodInvocationReport methodInvocationReport) {
        final DescribedInvocation describedInvocation = methodInvocationReport.getInvocation();
        if (describedInvocation instanceof Invocation) {
          this.callCounters.computeIfPresent(((Invocation)describedInvocation).getMethod(), (m, i) -> i + 1);
        }
      }
    }
    final SetterListener setterListener = new SetterListener(getSetters());

    /*
     * Wrap the original SovereignDataSetConfig instance with a Mockito spy using the
     * InvocationListener created above.
     */
    @SuppressWarnings("unchecked")
    final SovereignDataSetConfig<Integer, SystemTimeReference> mockedConfig =
        mock(SovereignDataSetConfig.class, withSettings().name("config")
            .spiedInstance(config)
            .invocationListeners(setterListener)
            .defaultAnswer(CALLS_REAL_METHODS));

    /*
     * Call all of the SovereignDataSetConfig setters setting non-default values.
     * Use non-default values to assure that duplicate does its job.
     */
    mockedConfig.concurrency(15);
    mockedConfig.recordLockTimeout(15, TimeUnit.SECONDS);

    final VersionLimitStrategy<SystemTimeReference> mockVersionLimitStrategy = mock(VersionLimitStrategy.class);
    Class<? extends TimeReference<?>> t = mockVersionLimitStrategy.type();
    when(mockVersionLimitStrategy.type()).thenReturn(SystemTimeReference.class);
    mockedConfig.versionLimitStrategy(mockVersionLimitStrategy);
    mockedConfig.alias("NonDefault Alias");
    mockedConfig.offheapResourceName("resource-name");
    mockedConfig.resourceSize(SovereignDataSetConfig.StorageType.OFFHEAP, 18 * 1024 * 1024);
    mockedConfig.storage(mock(AbstractStorage.class));
    mockedConfig.versionLimit(18);
    mockedConfig.bufferStrategyType(Simple);
    mockedConfig.fairLocking(!config.isFairLocking());
    mockedConfig.diskDurability(SovereignDatasetDiskDurability.NONE);
    final TimeReferenceGenerator<SystemTimeReference> mockTimeReferenceGenerator = mock(TimeReferenceGenerator.class);
    when(mockTimeReferenceGenerator.type()).thenReturn(SystemTimeReference.class);
    mockedConfig.timeReferenceGenerator(mockTimeReferenceGenerator);

    /*
     * Format a list of setters that were not called and assert that the list is empty
     * (assuring that all were called).
     */
    final List<String> missingMethods = setterListener.callCounters.entrySet().stream()
        .filter(e -> e.getValue() == 0)
        .map(e -> {
          final Method setter = e.getKey();
          final StringBuilder sb = new StringBuilder();
          sb.append(setter.getName()).append('(');
          sb.append(Arrays.stream(setter.getParameterTypes()).map(Class::getSimpleName).collect(joining(",")));
          sb.append(')');
          return sb.toString();
        })
        .collect(toList());
    assertThat(missingMethods, is(empty()));

    final SovereignDataSetConfig<Integer, SystemTimeReference> duplicate = mockedConfig.duplicate();
    assertThat(duplicate, is(not(nullValue())));
    assertThat(duplicate, is(not(sameInstance(mockedConfig))));
    assertThat(duplicate, is(not(sameInstance(config))));

    /*
     * Ensure the getters of the copy return the same information.
     */
    final List<Method> getters = findGetters();
    assertThat(getters.size(), is(greaterThan(0)));
    for (final Method getter : getters) {
      assertThat(get(getter, duplicate), is(get(getter, mockedConfig)));
    }
  }

  @Test
  public void deserializeOriginalVersion() throws Exception {
    SovereignDataSetConfig<Integer, FixedTimeReference> config1 = new SovereignDataSetConfig<>(Type.INT, FixedTimeReference.class);
    config1.alias("alias");
    config1.offheapResourceName("offheap");
    config1.bufferStrategyType(Simple);
    config1.concurrency(2);
    config1.fairLocking(true);
    config1.diskDurability(SovereignDatasetDiskDurability.timed(1, TimeUnit.MINUTES));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream( baos );
    oos.writeObject( config1 );
    oos.close();

    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInput objectInput = new ObjectInputStream(bais);
    @SuppressWarnings("unchecked")
    SovereignDataSetConfig<Integer, FixedTimeReference> config = (SovereignDataSetConfig<Integer, FixedTimeReference>) objectInput.readObject();

    assertEquals("alias", config.getAlias());
    assertEquals("offheap", config.getOffheapResourceName());
    assertEquals(Simple, config.getBufferStrategyType());
    assertEquals(2, config.getConcurrency());
    assertEquals(true, config.isFairLocking());
    assertEquals(SovereignDatasetDiskDurability.timed(1, TimeUnit.MINUTES), config.getDiskDurability());

  }

  @Test
  public void testConcurrencyForBool() {
    SovereignDataSetConfig<Boolean, TestTimeReference> config = new SovereignDataSetConfig<>(Type.BOOL, TestTimeReference.class);

    assertThat(config.getConcurrency(), is(1));

    config = config.concurrency(16);

    assertThat(config.getConcurrency(), is(1));
  }

  /**
   * Attempts to get a reference to all setters for instance fields composing a {@code SovereignDataSetConfig}.
   * This method presumes the field setters have the same name as the fields.
   * <p>
   * If setters are needed beyond those matching the field names, they should be added to this method.
   *
   * @return the list of {@code Method} references
   */
  private static List<Method> getSetters() {
    final List<Method> setters = new ArrayList<>();

    Class<?> clazz = SovereignDataSetConfig.class;
    do {
      final Field[] declaredFields = clazz.getDeclaredFields();
      for (final Field field : declaredFields) {
        if (!Modifier.isStatic(field.getModifiers())) {
          setters.addAll(findSetters(field));
        }
      }
    } while ((clazz = clazz.getSuperclass()) != Object.class);

    return setters;
  }

  /**
   * Returns the list of non-static, public methods having the same name as the supplied field.
   *
   * @param field the field who's name to match
   *
   * @return the list, possibly empty, of methods having the same name as {@code field}
   */
  private static List<Method> findSetters(final Field field) {
    final List<Method> setters = new ArrayList<>();

    Class<?> clazz = SovereignDataSetConfig.class;
    do {
      final Method[] declaredMethods = clazz.getDeclaredMethods();
      for (final Method declaredMethod : declaredMethods) {
        final int modifiers = declaredMethod.getModifiers();
        if (Modifier.isPublic(modifiers)
            && !Modifier.isStatic(modifiers)
            && declaredMethod.getName().equals(field.getName())) {
          setters.add(declaredMethod);
        }
      }
    } while ((clazz = clazz.getSuperclass()) != Object.class);

    return setters;
  }

  /**
   * Returns the list of non-static, public, niladic methods beginning with {@code get} or {@code is}.
   *
   * @return the {@code SovereignDataSetConfig} getter methods
   */
  private static List<Method> findGetters() {
    final List<Method> getters = new ArrayList<>();

    Class<?> clazz = SovereignDataSetConfig.class;
    do {
      final Method[] declaredMethods = clazz.getDeclaredMethods();
      for (final Method declaredMethod : declaredMethods) {
        final int modifiers = declaredMethod.getModifiers();
        if (Modifier.isPublic(modifiers)
            && !Modifier.isStatic(modifiers)
            && declaredMethod.getParameterTypes().length == 0
            && (declaredMethod.getName().startsWith("get") || declaredMethod.getName().startsWith("is"))) {
          getters.add(declaredMethod);
        }
      }
    } while ((clazz = clazz.getSuperclass()) != Object.class);

    return getters;
  }

  /**
   * Invoke the getter method provided against the {@code SovereignDataSetConfig} instance provided.
   *
   * @param getter the getter method to invoke
   * @param config the {@code SovereignDataSetConfig} instance
   *
   * @return the result of {@code getter}
   *
   * @throws InvocationTargetException if thrown by {@link Method#invoke(Object, Object...)}
   * @throws IllegalAccessException if thrown by {@link Method#invoke(Object, Object...)}
   */
  private Object get(final Method getter, final SovereignDataSetConfig<?, ?> config)
      throws InvocationTargetException, IllegalAccessException {
    return getter.invoke(config);
  }

  private static final class TestTimeReference implements TimeReference<TestTimeReference> {
    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return 0;
    }

    public static final class Generator implements TimeReferenceGenerator<TestTimeReference> {
      private static final long serialVersionUID = -3752883649815067211L;

      @Override
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }

      @Override
      public TestTimeReference get() {
        return new TestTimeReference();
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }
    }

    public static final class LimitStrategy implements VersionLimitStrategy<TestTimeReference> {
      private static final long serialVersionUID = 2704343466672387983L;

      @Override
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }

      @Override
      public BiFunction<TestTimeReference, TestTimeReference, Retention> getTimeReferenceFilter() {
        throw new UnsupportedOperationException();
      }
    }
  }
}
