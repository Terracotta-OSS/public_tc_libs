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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.exceptions.CellNameCollisionException;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.spi.store.PersistentRecord;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import org.hamcrest.core.IsNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.sovereign.impl.RoomSchema.available;
import static com.terracottatech.sovereign.impl.RoomSchema.owner;
import static com.terracottatech.sovereign.impl.RoomSchema.price;
import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests a few {@link SovereignDataset} operations.  This class must be subclassed
 * by classes providing several dataset configurations.
 *
 * @author Alex Snaps
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class AbstractDatasetApiTest {

  /**
   * Default space limit for datasets used in this test class.
   */
  protected static final int RESOURCE_SIZE = 1024 * 1024;

  private static Iterable<Cell<?>> replace(Record<?> record, Cell<?> value) {
    ArrayList<Cell<?>> list = new ArrayList<>();
    for (Cell<?> item : record) {
      if (item.definition().name().equals(value.definition().name())) {
        list.add(value);
      } else {
        list.add(item);
      }
    }
    return list;
  }

  protected abstract <K extends Comparable<K>> SovereignDataset<K> createDataset(SovereignDataSetConfig<K, ?> config)
    throws Exception;

  @SuppressWarnings("unchecked")
  @Test(expected = ClassCastException.class)
  @Ignore("SovereignDataset.get does not check type")
  public void testIsErasureSafe() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      dataset.get("foo");    // unchecked -- testing type failure
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testBasicDelete() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      CellDefinition<String> foo = define("foo", Type.STRING);
      CellDefinition<String> bar = define("bar", Type.STRING);
      dataset.add(IMMEDIATE, "0", foo.newCell("bar"), bar.newCell("foo"));
      dataset.delete(IMMEDIATE, "0");
      assertThat(dataset.get("0"), IsNull.nullValue());
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testCellNameCollisionsFailOnAddTest() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      try {
        dataset.add(IMMEDIATE, "key", cell("foo", 12L), cell("foo", "value"));
        fail("No cell name collision exception");
      } catch (CellNameCollisionException e) {
        // Expected
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testCellNameCollisionsFailOnMutationTest() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      final String key = "foobarbaz";

      // Create some values
      final CellDefinition<Integer> bar = define("bar", Type.INT);
      final CellDefinition<Long> baz = define("baz", Type.LONG);
      final CellDefinition<Integer> baz2 = define("baz", Type.INT);

      dataset.add(IMMEDIATE, key, cell("foobar", 10));

      // this is ok. re[placing one type with another is legal
      dataset.applyMutation(IMMEDIATE, key, r -> true, add(baz2.newCell(121)));

      // this is not, using two different types for the same name
      try {
        dataset.applyMutation(IMMEDIATE, key, r -> true, current -> {
          return Arrays.asList(new Cell<?>[] { baz2.newCell(121), baz.newCell(100L) });
        });
        fail();
      } catch (CellNameCollisionException e) {
        // Expected
      }
      try {
        dataset.applyMutation(IMMEDIATE, key, r -> true, current -> {
          return Arrays.asList(new Cell<?>[] { baz2.newCell(121), baz.newCell(100L) });
        });
        fail();
      } catch (CellNameCollisionException e) {
        // Expected
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testBasicPutGet() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      dataset.add(IMMEDIATE, "key", cell("foo", 12L), cell("bar", "value"));
      assertThat(dataset.get("key").get(define("foo", Type.LONG)).get(), is(12L));
      assertThat(dataset.get("key").get(define("bar", Type.STRING)).get(), is("value"));
      assertThat(dataset.get("key").get(define("foobar", Type.STRING)), is(empty()));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testBasicCompute() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      CellDefinition<Long> value = define("value", Type.LONG);
      CellDefinition<Boolean> filter = define("filter", Type.BOOL);

      assertThat(dataset.add(IMMEDIATE, "key1", value.newCell(14L), filter.newCell(true)), nullValue());
      assertThat(dataset.add(IMMEDIATE, "key2", value.newCell(16L), filter.newCell(true)), nullValue());
      assertThat(dataset.add(IMMEDIATE, "key2", value.newCell(16L), filter.newCell(true)), notNullValue());
      assertThat(dataset.add(IMMEDIATE, "key3", value.newCell(Long.MAX_VALUE), filter.newCell(false)), nullValue());

      OptionalDouble avg;

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        avg = recordStream.filter(row -> row.get(filter).get()).mapToLong(row -> row.get(value).orElse(0L)).average();
      }

      assertThat(avg.getAsDouble(), is(15d));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void predicatedRemove() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      CellDefinition<Long> value = define("value", Type.LONG);
      CellDefinition<Boolean> filter = define("filter", Type.BOOL);
      assertThat(dataset.add(IMMEDIATE, "key1", value.newCell(14L), filter.newCell(true)), nullValue());
      assertThat(dataset.add(IMMEDIATE, "key2", value.newCell(14L), filter.newCell(false)), nullValue());

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        assertThat(recordStream.count(), is(2L));
      }

      assertThat(dataset.delete(IMMEDIATE, "key1", row -> row.get(filter).get()), notNullValue());
      assertThat(dataset.delete(IMMEDIATE, "key2", row -> row.get(filter).get()), nullValue());

      dataset.records().forEach(System.out::println);
      try (final Stream<Record<String>> recordStream = dataset.records()) {
        assertThat(recordStream.count(), is(1L));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  // TAB-5743
  @Test
  public void testCrossCellComparison() throws Exception {
    SovereignDataset<Long> dataset = createDataset(new SovereignDataSetConfig<>(Type.LONG, FixedTimeReference.class));
    try {
      DoubleCellDefinition foo = CellDefinition.defineDouble("foo");
      DoubleCellDefinition bar = CellDefinition.defineDouble("bar");
      dataset.getIndexing().createIndex(foo, SovereignIndexSettings.btree()).call();
      dataset.getIndexing().createIndex(bar, SovereignIndexSettings.btree()).call();

      dataset.add(IMMEDIATE, 0L, foo.newCell(1.0), bar.newCell(1.0));
      dataset.add(IMMEDIATE, 1L, foo.newCell(1.0), bar.newCell(2.0));
      dataset.add(IMMEDIATE, 3L, foo.newCell(2.0), bar.newCell(1.0));
      dataset.add(IMMEDIATE, 2L, foo.newCell(1.0), bar.newCell(3.0));
      dataset.add(IMMEDIATE, 4L, foo.newCell(2.0), bar.newCell(2.0));
      dataset.add(IMMEDIATE, 6L, foo.newCell(3.0), bar.newCell(1.0));
      dataset.add(IMMEDIATE, 5L, foo.newCell(2.0), bar.newCell(3.0));
      dataset.add(IMMEDIATE, 7L, foo.newCell(3.0), bar.newCell(2.0));
      dataset.add(IMMEDIATE, 8L, foo.newCell(3.0), bar.newCell(3.0));

      try (final Stream<Record<Long>> recordStream = dataset.records()) {
        assertThat(
          recordStream.filter(r -> r.get(foo).flatMap(f -> r.get(bar).map(b -> f > b)).orElse(false)).count(),
          is(3L));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void playWithBuckets() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      // Some key
      final String key = "foobarbaz";

      // Define some cells (should rename define, field, attribute, typedef)
      final CellDefinition<Integer> bar = define("bar", Type.INT); // IntegerCell implements Cell<Integer>
      final CellDefinition<String> foo = define("foo", Type.STRING);
      final CellDefinition<Long> baz = define("baz", Type.LONG);
      final CellDefinition<Long> baz2 = define("baz2", Type.LONG);

      // Add them to the store
      if (dataset.add(IMMEDIATE, key, bar.newCell(120), foo.newCell("Thingy"), baz.newCell(Long.MAX_VALUE)) != null) {
        throw new IllegalStateException("Key is already in the bucket: " + key);
      }

      // Blindly update bar, adds baz2 & baz3... really? update, alter, ...?
      dataset.applyMutation(IMMEDIATE,
                            key,
                            r -> true,
                            add(bar.newCell(121), baz2.newCell(Long.MIN_VALUE), cell("baz3", 0.1d)));

      int i = 10;

      // increment bar and decrement baz2
      dataset.applyMutation(IMMEDIATE,
                            key,
                            r -> true,
                            add(row -> bar.newCell(row.get(bar).get() + i), row -> baz2.newCell(row.get(baz2).get() - row.get(baz).get())));

      // Retrieve the Row
      final Record<String> ourRecord = dataset.get(
        key); // should this getter specify the laziness of faulting cells

      for (Cell<?> value : ourRecord) {
        System.out.println(value.definition().name() + " (" + value.value().getClass() + "): " + value.value());
      }

      assertTrue(dataset.get(key).get(baz2).isPresent());
      dataset.applyMutation(IMMEDIATE, key, r -> true, remove(baz2));
      assertFalse(dataset.get(key).get(baz2).isPresent());
      int someValue = ourRecord.get(bar).get();

      // Delete the row
      assertThat(dataset.delete(IMMEDIATE, key, row -> row.get(bar).get() == 131), notNullValue());
      dataset.delete(IMMEDIATE, key); // deletes whatever
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testVersioning() throws Exception {
    SovereignDataset<Integer> dataset = createDataset(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class));
    try {
      fillBucket(dataset);
      Record<Integer> room = dataset.get(238);
      Assert.assertEquals(room.get(new CellDefinition<Double>() {

        @Override
        public String name() {
          return "price";
        }

        @Override
        public Type<Double> type() {
          return Type.DOUBLE;
        }

        @Override
        public Cell<Double> newCell(Double value) {
          return null;
        }
      }).get(), 500d, .1d);
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void reserveRoom() throws Exception {
    SovereignDataset<Integer> dataset = createDataset(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class));
    try {
      final long versionLimit = ((SovereignDatasetImpl) dataset).getConfig().getVersionLimit();
      fillBucket(dataset);

      if (versionLimit > 1) {
        // Make sure we do GC old versions
        assertThat(dataset.get(238).versions().count(), is(2L));
        assertThat(dataset.get(238).versions().reduce((current, previous) -> previous).get().get(price).get(), is(2000d));
        assertThat(dataset.get(238).versions().findFirst().get().get(price).get(), is(500d));
        assertThat(dataset.get(238).versions().filter(t -> false).count(), is(0L));
      }

      // Fetch room 237
      final SovereignRecord<Integer> room = dataset.get(237);
      if (versionLimit > 1) {
        assertThat(room.versions().filter(r -> r.get(available).isPresent()).count(), is(2L));
        assertThat(room.versions().filter(r -> r.get(price).isPresent()).count(), is(1L));
        assertThat(room.versions().filter(r -> r.get(owner).isPresent()).count(), is(0L));
      }
      final SovereignRecord<Integer> last = room.versions().filter(
        r -> r.get(available).isPresent()).findFirst().get();
      assertThat(((SystemTimeReference)last.getTimeReference()).getTime(), not(0L));
      assertThat(last.getTimeReference(), comparesEqualTo(room.getTimeReference()));
      if (last instanceof PersistentRecord && room instanceof PersistentRecord) {
        assertThat(last.getMSN(), is(room.getMSN()));
      }
      Optional<String> token = null;

      // Is it available?
      if (room.get(available).get()) {

        // What's the price ?
        System.out.println(room.get(price));

        // Try and reserve it, hoping for nobody to be any quicker
        token = dataset.applyMutation(IMMEDIATE, 237, r -> true, row -> {
          if (row.get(available).get()) {
            return AbstractDatasetApiTest.<Integer, FixedTimeReference>add(available.newCell(false),
              owner.newCell(UUID.randomUUID().toString())).apply(row);
          }
          return row;
        }, (previousRecord, newRecord) -> {
          if (previousRecord.get(available).get() && !newRecord.get(available).get()) {
            return newRecord.get(owner);
          } else {
            return Optional.<String>empty();
          }
        }).orElse(Optional.empty());

        // Did we succeed ?
        if (token.isPresent()) {
          System.out.println("Successfully reserved room, here's your token : " + token.get());
        } else {
          System.out.println("Successfully reserved room : nope!");
        }
      }
      assertThat(token, notNullValue());
      assertThat(token.isPresent(), is(true));
      assertThat(room.get(available).get(), is(true));
      assertThat(dataset.get(237).get(available).get(), is(false));
      assertThat(dataset.get(237).get(owner).get(), is(token.get()));

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void updateStuff() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      // Some key
      final String key = "foobarbaz";

      // Create some values
      final CellDefinition<Integer> bar = define("bar", Type.INT);
      final CellDefinition<Long> baz2 = define("baz2", Type.LONG);
      final CellDefinition<Long> baz = define("baz", Type.LONG);

      dataset.add(IMMEDIATE, key, bar.newCell(10), baz2.newCell(120L));

      dataset.applyMutation(IMMEDIATE,
                            key,
                            r -> true,
                            add(row -> bar.newCell(row.get(bar).get() + 10), row -> baz2.newCell(row.get(baz2).get() - row.get(bar).get()),
          row -> cell("baz", 12L)));

      final Record<String> record = dataset.get(key);
      assertThat(record.get(bar).get(), is(20));
      assertThat(record.get(baz2).get(), is(110L));
      assertThat(record.get(baz).get(), is(12L));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testConditionalMutationByKey() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {

      // Create some values
      final CellDefinition<Integer> cnt1Def = define("cnt1", Type.INT);
      final CellDefinition<Integer> cnt2Def = define("cnt2", Type.INT);

      dataset.add(IMMEDIATE, "key1", cnt1Def.newCell(10), cnt2Def.newCell(100));
      dataset.add(IMMEDIATE, "key2", cnt1Def.newCell(9), cnt2Def.newCell(99));

      for(String k:Arrays.asList("key1", "key2")) {
        dataset.applyMutation(IMMEDIATE,
                              k,
                              r -> ((r.get(cnt1Def).get() & 0x01) == 0),
                              (rec) -> {
                                int two = rec.get(cnt2Def).get();
                                return Arrays.asList(cnt1Def.newCell(rec.get(cnt1Def).get()), cnt2Def.newCell(two + 1));
                              });
      }

      assertThat(dataset.get("key1").get(cnt2Def).get(), is(101));
      assertThat(dataset.get("key2").get(cnt2Def).get(), is(99));

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void streamAPI() throws Exception {
    final CellDefinition<Integer> bar = define("bar", Type.INT);
    final CellDefinition<String> foo = define("foo", Type.STRING);

    SovereignDataset<Integer> dataset = createDataset(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class));
    try {
      fillBucket(dataset);
      Object annoyingCustomer = null;

      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        final long amountOfRooms = recordStream.count();
      }

      final Optional<Room> mostExpensiveRoom;
      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        // amongst all available room
        mostExpensiveRoom = recordStream.filter(row -> row.get(available).get())
           .map(row -> new Room(row.getKey(), row.get(available).get(), row.get(price).get(),row.get(owner).orElse(null))) // map to domain
           .filter(room -> room.appropriateFor(annoyingCustomer))                                  // filter based on complex business logic
           .sorted((r1, r2) -> r2.getPrice().compareTo(r1.getPrice()))                             // most to least expensive
           .findFirst();                                                                           // get the most expensive one
      }

      if (mostExpensiveRoom.isPresent()) {
        Room room = mostExpensiveRoom.get();
        System.out.println("Here's the best room for you sir: " + room);
      } else {
        System.out.print("Sorry no room for you right now");
      }

      assertThat(mostExpensiveRoom.isPresent(), is(true));
      assertThat(mostExpensiveRoom.get().getNumber(), is(237));

    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testBulkDelete() throws Exception {
    SovereignDataset<Integer> dataset = createDataset(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class));
    try {
      fillBucket(dataset);

      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        recordStream.filter(row -> row.get(available).get()).filter(row -> true).forEach(
          row -> dataset.delete(IMMEDIATE, row.getKey()));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  protected SovereignDataset<Integer> fillBucket(
    SovereignDataset<Integer> rooms) {
    rooms.add(IMMEDIATE, 240, available.newCell(true), owner.newCell("foo"), price.newCell(4000d));
    rooms.add(IMMEDIATE, 239, available.newCell(false), price.newCell(4000d));
    rooms.add(IMMEDIATE, 238, available.newCell(true), price.newCell(1000d));
    rooms.applyMutation(IMMEDIATE, 238, r -> true, add(available.newCell(false), price.newCell(2000d)));
    rooms.applyMutation(IMMEDIATE, 238, r -> true, add(available.newCell(true), price.newCell(500d)));
    rooms.add(IMMEDIATE, 237, available.newCell(false));
    rooms.applyMutation(IMMEDIATE, 237, r -> true, add(available.newCell(true), price.newCell(2000d)));
    return rooms;
  }

  public static Function<Iterable<Cell<?>>, Iterable<Cell<?>>> add(Cell<?>... values) {
    return current -> {
      Map<String, Cell<?>> newCells = new HashMap<>();
      for (Cell<?> currentValue : current) {
        newCells.put(currentValue.definition().name(), currentValue);
      }
      for (Cell<?> value : values) {
        newCells.put(value.definition().name(), value);
      }
      return newCells.values();
    };
  }

  @SafeVarargs
  public static <K extends Comparable<K>> Function<Record<K>, Iterable<Cell<?>>> add(
    Function<Record<K>, Cell<?>>... generators) {
    return record -> {
      Function<Iterable<Cell<?>>, Iterable<Cell<?>>> transform = Function.identity();
      for (Function<Record<K>, Cell<?>> generator : generators) {
        transform = transform.andThen(add(generator.apply(record)));
      }
      return transform.apply(record);
    };
  }

  public static <T> Function<Iterable<Cell<?>>, Iterable<Cell<?>>> remove(CellDefinition<?>... definitions) {
    return record -> {
      Map<String, Cell<?>> newCells = new HashMap<>();
      for (Cell<?> currentValue : record) {
        newCells.put(currentValue.definition().name(), currentValue);
      }
      for (CellDefinition<?> definition : definitions) {
        newCells.remove(definition.name());
      }
      return newCells.values();
    };
  }

  @Test
  public void multiThreaded() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      ThreadGroup tg = new ThreadGroup("tests");
      Thread[] list = new Thread[1];
      List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
      for (int x = 0; x < list.length; x++) {
        list[x] = new Thread(tg, createRunnable(tg, dataset, errors), "Thread-" + x);
        list[x].start();
      }
      for (Thread t : list) {
        t.join();
      }
      Optional<Throwable> t = errors.stream().findAny();
      if (t.isPresent()) {
        throw new RuntimeException(t.get());
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void readConsistencyTest() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      IntCellDefinition value = defineInt("filter");
      dataset.add(IMMEDIATE, "one", value.newCell(1));

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        recordStream.filter(value.value().isGreaterThan(0)).forEach(
          (r) -> dataset.applyMutation(IMMEDIATE, r.getKey(), r1 -> true, add(value.newCell(r.get(value).get() + 1))));
      }

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        recordStream.forEach(r -> System.out.println(r.get(value)));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void testSelfMutator() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      IntCellDefinition value = defineInt("counter");
      dataset.add(IMMEDIATE, "key1", value.newCell(1));

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        recordStream.forEach(r -> System.out.println(r.get(value)));
      }

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        recordStream.filter(value.value().isGreaterThan(0)).forEach(
          dataset.applyMutation(IMMEDIATE, r -> replace(r, value.newCell(r.get(value).get() + 1))));
      }

      try (final Stream<Record<String>> recordStream = dataset.records()) {
        recordStream.forEach(r -> System.out.println(r.get(value)));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Test
  public void multiMutator() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      ThreadGroup tg = new ThreadGroup("tests");
      Thread[] list = new Thread[32];
      List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
      CellDefinition<Boolean> value = define("filter", Type.BOOL);
      dataset.add(IMMEDIATE, "key1", value.newCell(Boolean.TRUE));
      for (int x = 0; x < list.length; x++) {
        list[x] = new Thread(tg, createMutator(tg, dataset, errors), "Thread-" + x);
        list[x].start();
      }
      for (Thread t : list) {
        t.join();
      }
      Optional<Throwable> t = errors.stream().findAny();
      if (t.isPresent()) {
        throw new RuntimeException(t.get());
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  private static Runnable createMutator(ThreadGroup killer, final SovereignDataset<String> dataset,
                                 final List<Throwable> elog) {
    return () -> {
      for (int x = 0; x < 1000; x++) {
        try {
          CellDefinition<Boolean> filter = define("filter", Type.BOOL);
          if (x % 2 == 0) {
            Optional<Boolean> result = dataset.applyMutation(SovereignDataset.Durability.LAZY, "key1", r -> true, (r) -> {
              List<Cell<?>> a = new ArrayList<>();
              a.add(filter.newCell(!r.get(filter).get()));
              return a;
            }, (r1, r2) -> r2.get(filter).get());
          }
        } catch (Throwable t) {
          elog.add(t);
          killer.interrupt();
          return;
        }
      }
    };
  }

  private static Runnable createRunnable(ThreadGroup killer, final SovereignDataset<String> dataset,
                                  final List<Throwable> elog) {
    return () -> {
      String name = Thread.currentThread().getName() + "_";
      for (int x = 0; x < 100; x++) {
        try {
          CellDefinition<Long> value = define("value", Type.LONG);

          CellDefinition<Boolean> filter = define("filter", Type.BOOL);
          assertThat(dataset.add(IMMEDIATE, name + "key1", value.newCell(14L), filter.newCell(true)), nullValue());
          assertThat(dataset.add(IMMEDIATE, name + "key2", value.newCell(14L), filter.newCell(false)), nullValue());

          try (final Stream<Record<String>> recordStream = dataset.records()) {
            assertThat(name, recordStream.filter(row -> row.getKey().startsWith(name)).count(), is(2L));
          }

          assertThat(dataset.delete(IMMEDIATE, name + "key1", row -> row.get(filter).get()), notNullValue());
          try {
            TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
          assertThat(dataset.delete(IMMEDIATE, name + "key2", row -> row.get(filter).get()), nullValue());

          try (final Stream<Record<String>> recordStream = dataset.records()) {
            assertThat(recordStream.filter(row -> row.getKey().startsWith(name)).count(), is(1L));
          }

          try (final Stream<Record<String>> recordStream = dataset.records()) {
            recordStream.filter(row -> row.getKey().startsWith(name)).forEach(
              (row) -> dataset.delete(IMMEDIATE, row.getKey()));
          }
          try {
            TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        } catch (Throwable t) {
          elog.add(t);
          killer.interrupt();
          return;
        }
      }
    };
  }

  @Test
  public void testMoreThan64k() throws Exception {
    SovereignDataset<String> dataset = createDataset(new SovereignDataSetConfig<>(Type.STRING, FixedTimeReference.class));
    try {
      CellDefinition<String> def = CellDefinition.define("name", Type.STRING);
      for (long i = 0; i < 70000; i++) {
        dataset.add(SovereignDataset.Durability.LAZY, "Key" + i, def.newCell("cell+" + i));
      }
      assertThat(dataset.records().count(), is(70000L));
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }
}
