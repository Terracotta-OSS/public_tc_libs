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

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import java.util.Map;

import static com.terracottatech.sovereign.testsupport.EmployeeSchema.MAX_EMPLOYEE_RECORDS;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.employeeCellDefinitionMap;
import static com.terracottatech.sovereign.testsupport.EmployeeSchema.idx;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class that mocks record data container, index map and catalog and runs
 * various stream usage tests, purely focusing on the streams layer and the
 * {@link SovereignOptimizer}.
 * <p/>
 * All the test cases are in {@link StreamUsageTest}. This class handles the mocking setup and
 * data loading necessary to run the tests in {@code StreamUsageTest}.
 *
 * @author RKAV
 */
public class RecordStreamUsageTest extends StreamUsageTest {

  @Mock
  private SovereignContainer<Integer> data;
  @Mock
  private SovereignSortedIndexMap<Integer, Integer> map;
  @Mock
  private Catalog<Integer> c;

  public RecordStreamUsageTest() {
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(data.get(any(PersistentMemoryLocator.class))).then(
      (InvocationOnMock top) -> getSimulatedRecord(((MockLocator) top.getArguments()[0]).getIndex()));
    when(c.getContainer()).thenReturn((SovereignContainer) data);
    when(this.data.start(true)).thenReturn(mock(ContextImpl.class));
    when(map.higher(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
        (int) invocation.getArguments()[1] + 1, MAX_EMPLOYEE_RECORDS - 1));
    when(map.get(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
                                                       (int) invocation.getArguments()[1], MAX_EMPLOYEE_RECORDS - 1));
    when(map.higherEqual(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
                                                       (int) invocation.getArguments()[1], MAX_EMPLOYEE_RECORDS - 1));
    when(map.lower(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.REVERSE,
        (int) invocation.getArguments()[1] - 1, MAX_EMPLOYEE_RECORDS - 1));
    when(map.lowerEqual(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.REVERSE,
        (int) invocation.getArguments()[1], MAX_EMPLOYEE_RECORDS - 1));
    when(map.get(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
        (int) invocation.getArguments()[1], MAX_EMPLOYEE_RECORDS - 1));
    when(c.getContainer()).thenReturn((SovereignContainer) data);
    when(c.hasSortedIndex(idx)).thenReturn(Boolean.TRUE);
    when(c.getSortedIndexFor(idx)).thenReturn(map);
    when(this.data.first(any(ContextImpl.class))).thenReturn(
      new MockLocator(Locator.TraversalDirection.FORWARD, 0, MAX_EMPLOYEE_RECORDS - 1));
  }

  @Override
  protected RecordStream<Integer> getRecordStream() {
    return RecordStreamImpl.newInstance(c, false);
  }

  /**
   * Verify that the streams spliterator has iterated through data only the specified times
   *
   * @param times the number of times records are got from the data container.
   */
  @Override
  protected void verifyData(int times) {
    verify(data, times(times)).get(ArgumentMatchers.any());
  }

  /**
   * Gets a record with the matching values simulated from the CSV file..
   *
   * @param index index into the data loaded from CSV file
   * @return a mocked record whose get(s) will return correct values
   */
  @SuppressWarnings("unchecked")
  private Record<?> getSimulatedRecord(int index) {
    Record<?> r = mock(VersionedRecord.class);
    Map<String, Object> recordMap = loadedData.get(index);

    when(r.get(ArgumentMatchers.eq(idx))).thenReturn(of(index));

    // ensure that the mock record returns correct value for the cell based on the CSV entry
    employeeCellDefinitionMap.forEach((k, v) -> {
      Object o = recordMap.get(k);
      if (o != null) {
        if (o instanceof Integer) {
          Integer i = (Integer) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<Integer>) v))).thenReturn(of(i));
        } else if (o instanceof Long) {
          Long l = (Long) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<Long>) v))).thenReturn(of(l));
        } else if (o instanceof Double) {
          Double d = (Double) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<Double>) v))).thenReturn(of(d));
        } else if (o instanceof Boolean) {
          Boolean b = (Boolean) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<Boolean>) v))).thenReturn(of(b));
        } else if (o instanceof byte[]) {
          byte[] b = (byte[]) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<byte[]>) v))).thenReturn(of(b));
        } else if (o instanceof String) {
          String s = (String) o;
          when(r.get(ArgumentMatchers.eq((CellDefinition<String>) v))).thenReturn(of(s));
        } else {
          when(r.get(ArgumentMatchers.eq(v))).thenReturn(empty());
        }
      } else {
        when(r.get(ArgumentMatchers.eq(v))).thenReturn(empty());
      }
    });
    return r;
  }

  static class MockLocator extends PersistentMemoryLocator {

    private final int index;
    private final int last;
    private final TraversalDirection direction;

    MockLocator(TraversalDirection direction, int index, int end) {
      super(index, null);
      this.index = index;
      this.direction = direction;
      this.last = end;
    }

    @Override
    public PersistentMemoryLocator next() {
      if (direction.isForward()) {
        if (index > last) {
          throw new ArrayIndexOutOfBoundsException("invalid");
        }
        return new MockLocator(direction, index + 1, last);
      } else if (direction.isReverse()) {
        if (index < 0) {
          throw new ArrayIndexOutOfBoundsException("invalid");
        }
        return new MockLocator(direction, index - 1, last);
      }
      return PersistentMemoryLocator.INVALID;
    }

    @Override
    public boolean isEndpoint() {
      return index == -1 || index == last + 1;
    }

    @Override
    public TraversalDirection direction() {
      return direction;
    }

    public int getIndex() {
      return index;
    }

  }
}
