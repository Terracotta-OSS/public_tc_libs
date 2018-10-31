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
package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.RecordData;
import com.terracottatech.store.internal.InternalRecord;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType.MAP;
import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType.RECORD;
import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType.forObject;
import static java.util.Objects.requireNonNull;

//TODO: this class has a lot in common with {@Link Element}. Both should be rationalized.
public class ElementValue {

  @FunctionalInterface
  public interface DataToRecordConverter {
    Record<?> toRecord(RecordData<?> r);
  }

  public enum ValueType {
    CELL {
      @Override
      public ElementValue createElementValue(Object o) {
        Cell<?> cell = (Cell<?>) o;
        return new ElementValue(CELL, new Primitive(cell.definition().name()), Collections.singletonList(new ElementValue(PRIMITIVE, new Primitive(cell), null)));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        String cellName = (String) elementValue.data.getValue();
        Object cellValue = elementValue.containees.get(0).getValue();
        return Cell.cell(cellName, cellValue);
      }
    },
    PRIMITIVE {
      @Override
      public ElementValue createElementValue(Object o) {
        return new ElementValue(PRIMITIVE, o != null ? new Primitive(o) : null, null);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return elementValue.data != null ? elementValue.data.getValue() : null;
      }
    },
    OPTIONAL_LONG {
      @Override
      public ElementValue createElementValue(Object o) {
        OptionalLong opt = (OptionalLong) o;
        return new ElementValue(OPTIONAL_LONG, opt.isPresent() ? new Primitive(opt.getAsLong()) : null, null);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return elementValue.data != null ? OptionalLong.of((Long) elementValue.data.getValue()) : OptionalLong.empty();
      }
    },
    OPTIONAL_INT {
      @Override
      public ElementValue createElementValue(Object o) {
        OptionalInt opt = (OptionalInt) o;
        return new ElementValue(OPTIONAL_INT, opt.isPresent() ? new Primitive(opt.getAsInt()) : null, null);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return elementValue.data != null ? OptionalInt.of((Integer) elementValue.data.getValue()) : OptionalInt.empty();
      }
    },
    OPTIONAL_DOUBLE {
      @Override
      public ElementValue createElementValue(Object o) {
        OptionalDouble opt = (OptionalDouble) o;
        return new ElementValue(OPTIONAL_DOUBLE, opt.isPresent() ? new Primitive(opt.getAsDouble()) : null, null);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return elementValue.data != null ? OptionalDouble.of((Double) elementValue.data.getValue()) : OptionalDouble.empty();
      }
    },
    OPTIONAL {
      @Override
      public ElementValue createElementValue(Object o) {
        Optional<?> opt = (Optional) o;
        Object param = opt.isPresent() ? opt.get() : null;
        return new ElementValue(OPTIONAL, null, Collections.singletonList(forObject(param).createElementValue(param)));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return elementValue.containees != null ? Optional.ofNullable(elementValue.containees.get(0).getValue(converter)) : Optional.empty();
      }
    },
    RECORD {
      @SuppressWarnings({"unchecked", "rawtypes"})
      @Override
      public ElementValue createElementValue(Object o) {
        InternalRecord<?> internalRecord = (InternalRecord<?>) o;
        return getElementValue(new RecordData(internalRecord.getMSN(), internalRecord.getKey(), internalRecord));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        requireNonNull(converter, "Record converter is required when working with records");
        return converter.toRecord(getRecordData(elementValue));
      }
    },
    INT_SUMMARY_STATISTICS {
      @Override
      public ElementValue createElementValue(Object o) {
        IntSummaryStatistics stats = (IntSummaryStatistics) o;
        return new ElementValue(INT_SUMMARY_STATISTICS, null, Arrays.asList(
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getCount()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getSum()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMin()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMax()), null)
        ));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        IntSummaryStatistics intStats = new IntSummaryStatistics();
        setWithFieldReflection(intStats, INT_COUNT, elementValue.containees.get(0).getData().getValue());
        setWithFieldReflection(intStats, INT_SUM, elementValue.containees.get(1).getData().getValue());
        setWithFieldReflection(intStats, INT_MIN, elementValue.containees.get(2).getData().getValue());
        setWithFieldReflection(intStats, INT_MAX, elementValue.containees.get(3).getData().getValue());
        return intStats;
      }
    },
    LONG_SUMMARY_STATISTICS {
      @Override
      public ElementValue createElementValue(Object o) {
        LongSummaryStatistics stats = (LongSummaryStatistics) o;
        return new ElementValue(LONG_SUMMARY_STATISTICS, null, Arrays.asList(
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getCount()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getSum()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMin()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMax()), null)
        ));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        LongSummaryStatistics longStats = new LongSummaryStatistics();
        setWithFieldReflection(longStats, LONG_COUNT, elementValue.containees.get(0).getData().getValue());
        setWithFieldReflection(longStats, LONG_SUM, elementValue.containees.get(1).getData().getValue());
        setWithFieldReflection(longStats, LONG_MIN, elementValue.containees.get(2).getData().getValue());
        setWithFieldReflection(longStats, LONG_MAX, elementValue.containees.get(3).getData().getValue());
        return longStats;
      }
    },
    DOUBLE_SUMMARY_STATISTICS {
      @Override
      public ElementValue createElementValue(Object o) {
        DoubleSummaryStatistics stats = (DoubleSummaryStatistics) o;
        return new ElementValue(DOUBLE_SUMMARY_STATISTICS, null, Arrays.asList(
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getCount()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getSum()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMin()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(stats.getMax()), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(getWithFieldReflection(stats, DOUBLE_SUM_COMPENSATION)), null),
            new ElementValue(ValueType.PRIMITIVE, new Primitive(getWithFieldReflection(stats, DOUBLE_SIMPLE_SUM)), null)
        ));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        DoubleSummaryStatistics doubleStats = new DoubleSummaryStatistics();
        setWithFieldReflection(doubleStats, DOUBLE_COUNT, elementValue.containees.get(0).getData().getValue());
        setWithFieldReflection(doubleStats, DOUBLE_SUM, elementValue.containees.get(1).getData().getValue());
        setWithFieldReflection(doubleStats, DOUBLE_MIN, elementValue.containees.get(2).getData().getValue());
        setWithFieldReflection(doubleStats, DOUBLE_MAX, elementValue.containees.get(3).getData().getValue());
        setWithFieldReflection(doubleStats, DOUBLE_SUM_COMPENSATION, elementValue.containees.get(4).getData().getValue());
        setWithFieldReflection(doubleStats, DOUBLE_SIMPLE_SUM, elementValue.containees.get(5).getData().getValue());
        return doubleStats;
      }
    },
    RECORD_DATA_TUPLE {
      @Override
      public ElementValue createElementValue(Object o) {
        Tuple<?, ?> tuple = (Tuple<?, ?>)o;
        return new ElementValue(RECORD_DATA_TUPLE, null, Arrays.asList(
            getElementValue((RecordData<?>)tuple.getFirst()),
            getElementValue((RecordData<?>)tuple.getSecond())
        ));
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return Tuple.of((RecordData<?>)getRecordData(elementValue.containees.get(0)),
            (RecordData<?>)getRecordData(elementValue.containees.get(1)));
      }
    },
    MAP {
      @Override
      public ElementValue createElementValue(Object o) {
        return toMapValue(o, this);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return toMap(elementValue, HashMap::new, converter);
      }
    },
    CONCURRENT_MAP {
      @Override
      public ElementValue createElementValue(Object o) {
        return toMapValue(o, this);
      }

      @Override
      public Object getDataValue(ElementValue elementValue, DataToRecordConverter converter) {
        return toMap(elementValue, ConcurrentHashMap::new, converter);
      }
    };

    static ValueType forObject(Object o) {
      if (o == null || Type.forJdkType(o.getClass()) != null) {
        return ValueType.PRIMITIVE;
      } else if (o instanceof Cell) {
        return ValueType.CELL;
      } else if (o instanceof OptionalLong) {
        return ValueType.OPTIONAL_LONG;
      } else if (o instanceof OptionalInt) {
        return ValueType.OPTIONAL_INT;
      } else if (o instanceof OptionalDouble) {
        return ValueType.OPTIONAL_DOUBLE;
      } else if (o instanceof Optional) {
        return ValueType.OPTIONAL;
      } else if (o instanceof Record) {
        return ValueType.RECORD;
      } else if (o instanceof IntSummaryStatistics) {
        return ValueType.INT_SUMMARY_STATISTICS;
      } else if (o instanceof LongSummaryStatistics) {
        return ValueType.LONG_SUMMARY_STATISTICS;
      } else if (o instanceof DoubleSummaryStatistics) {
        return ValueType.DOUBLE_SUMMARY_STATISTICS;
      } else if (o instanceof Tuple
          && ((Tuple)o).getFirst() instanceof RecordData
          && ((Tuple)o).getSecond() instanceof RecordData) {
        /*
         * Supported for Tuple<RecordData<?>, RecordData<?>> where one neither element is null.
         */
        return RECORD_DATA_TUPLE;
      } else if (o instanceof ConcurrentMap) {
        return ValueType.CONCURRENT_MAP;
      } else if (o instanceof Map) {
        return ValueType.MAP;
      } else {
        throw new UnsupportedOperationException("Unsupported element value : " + o + " of " + o.getClass());
      }
    }

    public abstract <K extends Comparable<K>> ElementValue createElementValue(Object o);

    public abstract Object getDataValue(ElementValue elementValue, DataToRecordConverter converter);

  }

  /**
   * Converts a {@link RecordData} instance to an {@code ElementValue}.
   * @param recordData the {@code RecordData} instance to convert
   * @return an {@code ElementValue} holding the values from {@code recordData}
   */
  private static ElementValue getElementValue(RecordData<?> recordData) {
    List<ElementValue> values = new ArrayList<>();
    values.add(new ElementValue(ValueType.PRIMITIVE, new Primitive(recordData.getMsn()), null));
    for (Cell<?> cell : recordData.getCells()) {
      values.add(new ElementValue(ValueType.CELL, new Primitive(cell.definition().name()),
          Collections.singletonList(new ElementValue(ValueType.PRIMITIVE, new Primitive(cell), null))));
    }
    return new ElementValue(RECORD, new Primitive(recordData.getKey()), values);
  }

  /**
   * Converts an {@code ElementValue} to a {@link RecordData} instance.
   * @param elementValue the {@code ElementValue} to convert
   * @return a new {@code RecordData} instance holding the values from{@code elementValue}
   */
  private static <K extends Comparable<K>> RecordData<?> getRecordData(ElementValue elementValue) {
    @SuppressWarnings("unchecked") K key = (K)Comparable.class.cast(elementValue.data.getValue());
    Long msn = elementValue.containees.get(0).getValue();
    List<Cell<?>> cells = new ArrayList<>();
    for (int i = 1; i < elementValue.containees.size(); i++) {
      ElementValue ev = elementValue.containees.get(i);
      cells.add(ev.getValue());   // A cell can't contain a Record
    }
    return new RecordData<>(msn, key, cells);
  }

  private final ValueType valueType;
  private final Primitive data;
  private final List<ElementValue> containees;

  ElementValue(ValueType valueType, Primitive data, List<ElementValue> containees) {
    this.valueType = valueType;
    this.data = data;
    this.containees = containees;
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue() {
    return (T) valueType.getDataValue(this, null);
  }

  @SuppressWarnings("unchecked")
  public <T> T getValue(DataToRecordConverter converter) {
    return (T) valueType.getDataValue(this, requireNonNull(converter));
  }

  ValueType getValueType() {
    return valueType;
  }

  Primitive getData() {
    return data;
  }

  List<ElementValue> getContainees() {
    return containees;
  }

  public static ElementValue createForObject(Object obj) {
    return ElementValue.ValueType.forObject(obj).createElementValue(obj);
  }


  // You find the following code shitty? Yeah, me too. Go complain on JDK-8043747

  private static Field getDeclaredField(Class<?> target, String fieldName) {
    try {
      return AccessController.doPrivileged((PrivilegedExceptionAction<Field>) () -> {
        Field declaredField = target.getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return declaredField;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Object getWithFieldReflection(Object target, Field declaredField) {
    try {
      return AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> declaredField.get(target));
    } catch (PrivilegedActionException e) {
      throw new RuntimeException(e);
    }
  }

  private static void setWithFieldReflection(Object target, Field declaredField, Object value) {
    try {
      AccessController.doPrivileged((PrivilegedExceptionAction<Object>) () -> {
        declaredField.set(target, value);
        return null;
      });
    } catch (PrivilegedActionException e) {
      throw new RuntimeException(e);
    }
  }

  private static ElementValue toMapValue(Object o, ValueType valueType) {
    Map<?, ?> map = (Map<?, ?>) o;
    List<ElementValue> entryValues = map.entrySet()
            .stream()
            .map(ElementValue::toEntryValue)
            .collect(Collectors.toList());
    return new ElementValue(valueType, null, entryValues);
  }

  private static ElementValue toEntryValue(Map.Entry<?, ?> e) {
    return new ElementValue(MAP, null, Arrays.asList(
            toEntryElementValue(e.getKey()),
            toEntryElementValue(e.getValue())
    ));
  }

  private static ElementValue toEntryElementValue(Object o) {
    return forObject(o).createElementValue(o);
  }

  private static Map<?, ?> toMap(ElementValue elementValue, Supplier<Map<Object, Object>> supplier,
                                 DataToRecordConverter converter) {
    return elementValue.getContainees()
            .stream()
            .map(containee -> toEntry(containee, converter))
            .collect(supplier, (map, entry) -> map.put(entry.getKey(), entry.getValue()), Map::putAll);
  }

  private static Map.Entry<?, ?> toEntry(ElementValue elementValue, DataToRecordConverter converter) {
    Iterator<Object> keyValueIterator = elementValue.getContainees()
            .stream()
            .map(containee -> toEntryElement(containee, converter))
            .iterator();
    return new AbstractMap.SimpleEntry<>(keyValueIterator.next(), keyValueIterator.next());
  }

  private static Object toEntryElement(ElementValue elementValue, DataToRecordConverter converter) {
    return elementValue.valueType.getDataValue(elementValue, converter);
  }

  private static final Field INT_COUNT = getDeclaredField(IntSummaryStatistics.class, "count");
  private static final Field INT_SUM = getDeclaredField(IntSummaryStatistics.class, "sum");
  private static final Field INT_MIN = getDeclaredField(IntSummaryStatistics.class, "min");
  private static final Field INT_MAX = getDeclaredField(IntSummaryStatistics.class, "max");

  private static final Field LONG_COUNT = getDeclaredField(LongSummaryStatistics.class, "count");
  private static final Field LONG_SUM = getDeclaredField(LongSummaryStatistics.class, "sum");
  private static final Field LONG_MIN = getDeclaredField(LongSummaryStatistics.class, "min");
  private static final Field LONG_MAX = getDeclaredField(LongSummaryStatistics.class, "max");

  private static final Field DOUBLE_COUNT = getDeclaredField(DoubleSummaryStatistics.class, "count");
  private static final Field DOUBLE_SUM = getDeclaredField(DoubleSummaryStatistics.class, "sum");
  private static final Field DOUBLE_MIN = getDeclaredField(DoubleSummaryStatistics.class, "min");
  private static final Field DOUBLE_MAX = getDeclaredField(DoubleSummaryStatistics.class, "max");
  private static final Field DOUBLE_SUM_COMPENSATION = getDeclaredField(DoubleSummaryStatistics.class, "sumCompensation");
  private static final Field DOUBLE_SIMPLE_SUM = getDeclaredField(DoubleSummaryStatistics.class, "simpleSum");

}