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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.store.Record;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Spliterator;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Clifford W. Johnson
 */
public class ManagedSpliteratorTest extends RecordSpliteratorTest {
  @Mock
  protected MockAction mockAction;

  @Override
  @SuppressWarnings("unchecked")
  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    when(this.mockAction.getContainer()).thenReturn((SovereignContainer)this.container);    // unchecked
    when(this.mockAction.begin(any())).then(returnsFirstArg());
  }

  @Override
  protected Spliterator<Record<String>> getSpliteratorUnderTest(PersistentMemoryLocator locator) {
    return new ManagedSpliterator<>(null, container, locator, Long.MIN_VALUE, null, null);
  }

  protected ManagedSpliterator<String> getManagedSpliteratorUnderTestWithManagedAction() {
    return new ManagedSpliterator<>(null, this.container, null, Long.MIN_VALUE, null, mockAction);
  }

  @Test
  public void testClose() throws Exception {
    final ManagedSpliterator<String> spliterator = getManagedSpliteratorUnderTestWithManagedAction();
    spliterator.close();
    verify(this.mockAction).close();
  }

  @SuppressWarnings("try")
  private interface MockAction extends ManagedAction<String>, AutoCloseable {
  }
}
