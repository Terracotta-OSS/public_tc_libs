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

import org.junit.Test;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.exceptions.InvalidConfigException;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReferenceVersionLimitStrategy;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Type;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class SovereignBuilderTest {

  @Test
  public void testCtor() throws Exception {
    new SovereignBuilder<>(Type.INT, TestTimeReference.class);
  }

  @Test
  public void testCtorNullKeyType() throws Exception {
    try {
      new SovereignBuilder<>(null, TestTimeReference.class);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testCtorNullGeneratorType() throws Exception {
    try {
      new SovereignBuilder<>(Type.INT, null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testCtorConfig() throws Exception {
    final SovereignDataSetConfig<Integer, SystemTimeReference> config =
        new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class);
    new SovereignBuilder<>(config);
  }

  @Test
  public void testCtorNullConfig() throws Exception {
    try {
      new SovereignBuilder<>(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

  }

  @Test
  public void testCtorBadConfig() throws Exception {
    final SovereignDataSetConfig<Integer, TestTimeReference> incompleteConfig =
        new SovereignDataSetConfig<>(Type.INT, TestTimeReference.class);
    try {
      new SovereignBuilder<>(incompleteConfig);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCtorSupportedKeyTypes() throws Exception {
    for (final Type<?> keyType : new Type<?>[] {Type.BOOL, Type.CHAR, Type.DOUBLE, Type.INT, Type.LONG, Type.STRING}) {
      new SovereignBuilder(keyType, TestTimeReference.class);
    }

    try {
      new SovereignBuilder(Type.BYTES, TestTimeReference.class);
      fail();
    } catch (InvalidConfigException e) {
      // expected
    }
  }

  @Test
  public void testRecordLockTimeout() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    assertThat(builder.recordLockTimeout(1, TimeUnit.SECONDS), is(sameInstance(builder)));
  }

  @Test
  public void testNullRecordLockTimeout() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.recordLockTimeout(0 , null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testBadRecordLockTimeout() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.recordLockTimeout(-1, TimeUnit.SECONDS);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testVersionLimitStrategy() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    assertThat(builder.versionLimitStrategy(new TestTimeReference.LimitStrategy()), is(sameInstance(builder)));
  }

  @Test
  public void testNullVersionLimitStrategy() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.versionLimitStrategy(null);
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBadVersionLimitStrategy() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.versionLimitStrategy(
          (VersionLimitStrategy)new SystemTimeReferenceVersionLimitStrategy(0, TimeUnit.MILLISECONDS));  // intentionally cross-types
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testTimeReferenceGenerator() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    assertThat(builder.timeReferenceGenerator(new TestTimeReference.Generator()), is(sameInstance(builder)));
  }

  @Test
  public void testNullTimeReferenceGenerator() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.timeReferenceGenerator(null);
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBadTimeReferenceGenerator() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.timeReferenceGenerator((TimeReferenceGenerator)new SystemTimeReference.Generator());  // intentionally cross-types
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testMissingTimeReferenceGenerator() throws Exception {
    final SovereignBuilder<Integer, TestTimeReference> builder =
        new SovereignBuilder<>(Type.INT, TestTimeReference.class);
    try {
      builder.build();
      fail();
    } catch (InvalidConfigException e) {
      // expected
    }
  }

  @Test
  public void testDefaultTimeReferenceGenerator() throws Exception {
    final SovereignBuilder<Integer, SystemTimeReference> builder =
        new SovereignBuilder<>(Type.INT, SystemTimeReference.class);
    SovereignDataset<Integer> dataset = null;
    try {
      dataset = builder.build();
      assertThat(dataset.getTimeReferenceGenerator(), is(instanceOf(SystemTimeReference.Generator.class)));
    } finally {
      if (dataset != null) {
        dataset.getStorage().destroyDataSet(dataset.getUUID());
      }
    }
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
      private static final long serialVersionUID = 176222668386417854L;

      @Override
      public Class<TestTimeReference> type() {
        return TestTimeReference.class;
      }
    }
  }
}
