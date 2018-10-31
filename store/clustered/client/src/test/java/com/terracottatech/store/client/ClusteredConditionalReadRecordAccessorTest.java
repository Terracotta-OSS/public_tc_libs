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
package com.terracottatech.store.client;

import com.terracottatech.store.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClusteredConditionalReadRecordAccessorTest {

  @Mock
  private DatasetEntity<String> entity;

  @Mock
  private Predicate<? super Record<String>> predicate;

  @Mock
  private RecordImpl<String> record;

  @SuppressWarnings("unchecked")
  private final ArgumentCaptor<Record<String>> recordCaptor = ArgumentCaptor.forClass(Record.class);

  private ClusteredConditionalReadRecordAccessor<String> accessor;

  @Before
  public void before() {
    accessor = new ClusteredConditionalReadRecordAccessor<>("key", entity, predicate);
  }

  @Test
  public void recordFilteredTrue() {
    when(entity.get("key")).thenReturn(record);
    when(predicate.test(record)).thenReturn(true);

    Optional<Record<String>> result = accessor.read();
    assertEquals(record, result.get());

    assertRecordPassedToPredicate();
  }

  @Test
  public void recordFilteredFalse() {
    when(entity.get("key")).thenReturn(record);
    when(predicate.test(record)).thenReturn(false);

    Optional<Record<String>> result = accessor.read();
    assertFalse(result.isPresent());

    assertRecordPassedToPredicate();
  }

  @Test
  public void entityDoesNotReturnRecord() {
    when(entity.get("key")).thenReturn(null);

    Optional<Record<String>> result = accessor.read();
    assertFalse(result.isPresent());

    verifyNoMoreInteractions(predicate);
  }

  private void assertRecordPassedToPredicate() {
    verify(predicate).test(recordCaptor.capture());
    assertEquals(record, recordCaptor.getValue());
  }
}
