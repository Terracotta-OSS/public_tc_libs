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

package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig.RecordBufferStrategyType;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.recordstrategies.codec.VersionedRecordCodec;
import com.terracottatech.sovereign.impl.memory.recordstrategies.simple.SimpleRecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec.ValuePileRecordBufferStrategy;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.testsupport.RecordGenerator;
import com.terracottatech.sovereign.testsupport.SelectedTestRunner;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.sovereign.testsupport.TestUtility.formatMillis;
import static com.terracottatech.sovereign.testsupport.TestUtility.formatNanos;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * @author Clifford W. Johnson
 */
public class RecordBufferStrategyPerformanceTest {

  private static final Period TEST_DURATION = new Period(1, TimeUnit.MINUTES);
  private static final int MAX_RECORD_COUNT = 1500;

  /**
   * When this system property is set to {@code true}, the performance tests are run.
   */
  public static final String PERFORMANCE_TEST_PROPERTY = "com.terracottatech.sovereign.encoding.performance.test";

  /**
   * Reference to {@link ThreadMXBean}.  This reference is used to obtain statistics
   * about lock and thread performance.
   */
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  static {
    THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
  }

  private static final CompilationMXBean COMPILATION_MX_BEAN = ManagementFactory.getCompilationMXBean();

  private static final SystemTimeReference.Generator GENERATOR = new SystemTimeReference.Generator();

  @Rule
  public TestName testName = new TestName();

  private final String runPerformanceTests = System.getProperty(PERFORMANCE_TEST_PROPERTY, "false");

  private final List<VersionedRecord<?>> testRecords = new ArrayList<>(MAX_RECORD_COUNT);

  private Random rnd;
  private volatile boolean testComplete;

  @Before
  public void setUp() throws Exception {
    this.rnd = new Random(8675309L);
  }

  @After
  public void cleanUp() {
    this.testRecords.clear();
    final Runtime runtime = Runtime.getRuntime();
    for (int i = 0; i < 10; i++) {
      runtime.gc();
      runtime.runFinalization();
      Thread.yield();
    }
  }

  @Test
  public void testCodecCountString() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    Type<String> keyType = Type.STRING;
    timingAllCodecCount(keyType);
  }

  @Test
  public void testCodecCountInt() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    Type<Integer> keyType = Type.INT;
    timingAllCodecCount(keyType);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void timingAllCodecCount(Type<?> keyType) throws IOException {
    this.generateRecords(new RecordGenerator(keyType.getJDKType(), GENERATOR));
    for(RecordBufferStrategyType r: RecordBufferStrategyType.values()) {
      timingCount(keyType, r);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void timingCount(Type<?> keyType, RecordBufferStrategyType rtype) throws IOException {
    SovereignDatasetImpl sov = (SovereignDatasetImpl) new SovereignBuilder(keyType, SystemTimeReference.class).offheap()
      .bufferStrategy(rtype)
      .concurrency(4)
      .build();
    for (VersionedRecord<?> vr : testRecords) {
      Cell<?>[] cs = vr.cells().values().toArray(new Cell[0]);
      sov.add(SovereignDataset.Durability.LAZY, vr.getKey(), cs);
    }

    startTestTimer();

    long start = System.nanoTime();
    long cnt = 0;
    int loops = 0;
    while (!this.testComplete) {
      cnt = cnt + sov.records().count();
      loops++;
    }
    long took = System.nanoTime() - start;
    float perMS = (float)cnt / (TimeUnit.MILLISECONDS.convert(took, TimeUnit.NANOSECONDS));
    System.out.format("%s[%s:%s]: counted %d records (%d X %d loops); took=%dns; which is %.2f/ms.\n",
                      this.testName.getMethodName(),
                      keyType.getJDKType().getSimpleName(),
                      rtype,
                      cnt,
                      testRecords.size(),
                      loops,
                      took,
                      perMS);
    sov.getConfig().getStorage().shutdown();
  }

  private void startTestTimer() {
    this.testComplete = false;
    final Timer timer = new Timer("TestTimer", true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        RecordBufferStrategyPerformanceTest.this.testComplete = true;
        timer.cancel();
      }
    }, TEST_DURATION.asMillis());
  }

  @Test
  public void testVersionedRecordCodecString() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.STRING, new VersionedRecordCodec<>(GENERATOR));
  }

  @Test
  public void testVersionedRecordCodecLong() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.LONG, new VersionedRecordCodec<>(GENERATOR));
  }

  @Test
  public void testVersionedRecordCodecInt() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.INT, new VersionedRecordCodec<>(GENERATOR));
  }

  @Test
  public void testVersionedRecordCodecDouble() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.DOUBLE, new VersionedRecordCodec<>(GENERATOR));
  }

  @Test
  public void testVSLazyRecordBufferStrategyInt() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.INT,
                      new ValuePileRecordBufferStrategy<>(new SovereignDataSetConfig<>(Type.INT,
                                                                                       SystemTimeReference.class),
                                                          new DatasetSchemaImpl(),
                                                          true));
  }

  @Test
  public void testVSLazyRecordBufferStrategyString() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.STRING,
                      new ValuePileRecordBufferStrategy<String>(new SovereignDataSetConfig<>(Type.STRING,
                                                                                             SystemTimeReference.class),
                                                                new DatasetSchemaImpl(),
                                                                true));
  }

  @Test
  public void testVSLazyRecordBufferStrategyLong() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.LONG,
                      new ValuePileRecordBufferStrategy<Long>(new SovereignDataSetConfig<>(Type.LONG,
                                                                                           SystemTimeReference.class),
                                                              new DatasetSchemaImpl(),
                                                              true));
  }

  @Test
  public void testVSLazyRecordBufferStrategyDouble() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.DOUBLE,
                      new ValuePileRecordBufferStrategy<Double>(new SovereignDataSetConfig<>(Type.DOUBLE,
                                                                                             SystemTimeReference.class),
                                                                new DatasetSchemaImpl(),
                                                                true));
  }

  @Test
  public void testVSRecordBufferStrategyInt() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.INT,
                      new ValuePileRecordBufferStrategy<>(new SovereignDataSetConfig<>(Type.INT,
                                                                                       SystemTimeReference.class),
                                                          new DatasetSchemaImpl(),
                                                          false));
  }

  @Test
  public void testVSRecordBufferStrategyString() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.STRING,
                      new ValuePileRecordBufferStrategy<String>(new SovereignDataSetConfig<>(Type.STRING,
                                                                                             SystemTimeReference.class),
                                                                new DatasetSchemaImpl(),
                                                                false));
  }

  @Test
  public void testVSRecordBufferStrategyLong() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.LONG,
                      new ValuePileRecordBufferStrategy<Long>(new SovereignDataSetConfig<>(Type.LONG,
                                                                                           SystemTimeReference.class),
                                                              new DatasetSchemaImpl(),
                                                              false));
  }

  @Test
  public void testVSRecordBufferStrategyDouble() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.DOUBLE,
                      new ValuePileRecordBufferStrategy<Double>(new SovereignDataSetConfig<>(Type.DOUBLE,
                                                                                             SystemTimeReference.class),
                                                                new DatasetSchemaImpl(),
                                                                false));
  }

  @Test
  public void testSimpleRecordBufferStrategyString() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.STRING,
                      new SimpleRecordBufferStrategy<>(new SovereignDataSetConfig<>(Type.STRING,
                                                                                    SystemTimeReference.class),
                                                       new DatasetSchemaImpl()));
  }

  @Test
  public void testSimpleRecordBufferStrategyLong() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.LONG,
                      new SimpleRecordBufferStrategy<>(new SovereignDataSetConfig<>(Type.LONG,
                                                                                    SystemTimeReference.class),
                                                       new DatasetSchemaImpl()));
  }

  @Test
  public void testSimpleRecordBufferStrategyInt() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    this.timeEncoding(Type.INT,
                      new SimpleRecordBufferStrategy<>(new SovereignDataSetConfig<>(Type.INT,
                                                                                    SystemTimeReference.class),
                                                       new DatasetSchemaImpl()));
  }

  @Test
  public void testSimpleRecordBufferStrategyDouble() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));

    SovereignDataSetConfig<Double, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.DOUBLE,
                                                                                              SystemTimeReference.class);
    this.timeEncoding(Type.DOUBLE, new SimpleRecordBufferStrategy<>(config, new DatasetSchemaImpl()));
  }

  private <K extends Comparable<K>> void timeEncoding(final Type<K> keyType,
                                                      final RecordBufferStrategy<K> codec) {

    this.generateRecords(new RecordGenerator<>(keyType.getJDKType(), GENERATOR));
    this.timeEncoding(keyType.getJDKType(), codec);
  }

  private <K extends Comparable<K>> void timeEncoding(final Class<K> keyType,
                                                                                  final RecordBufferStrategy<K> codec) {
    int cellCount = 0;
    int versionCount = 0;
    long byteCount = 0;

    startTestTimer();

    final Map<String, GcObservation> startGcObservations = getGcObservations();
    final long startCompilationTime;
    if (COMPILATION_MX_BEAN.isCompilationTimeMonitoringSupported()) {
      startCompilationTime = COMPILATION_MX_BEAN.getTotalCompilationTime();
    } else {
      startCompilationTime = -1;
    }
    final long threadId = Thread.currentThread().getId();
    final long startCpuTime = THREAD_MX_BEAN.getThreadCpuTime(threadId);
    final long startUserTime = THREAD_MX_BEAN.getThreadUserTime(threadId);
    final long startElapsedTime = System.nanoTime();

    while (!this.testComplete) {
      @SuppressWarnings("unchecked") final VersionedRecord<K> versionedRecord = (VersionedRecord<K>) this.testRecords
        .get(this.rnd.nextInt(this.testRecords.size()));

      final ByteBuffer buffer = codec.toByteBuffer(versionedRecord);
      byteCount += buffer.limit();
      final K key = codec.readKey(buffer);
      final SovereignPersistentRecord<K> decodedRecord = codec.fromByteBuffer(buffer);

      versionCount += decodedRecord.elements().size();
      for (SovereignPersistentRecord<K> version : decodedRecord.elements()) {
        cellCount += version.cells().size();
      }
    }

    final long usedCpuTime = THREAD_MX_BEAN.getThreadCpuTime(threadId) - startCpuTime;
    final long usedUserTime = THREAD_MX_BEAN.getThreadUserTime(threadId) - startUserTime;
    final long elapsedTime = System.nanoTime() - startElapsedTime;
    final long totalCompilationTime;
    if (COMPILATION_MX_BEAN.isCompilationTimeMonitoringSupported()) {
      totalCompilationTime = COMPILATION_MX_BEAN.getTotalCompilationTime() - startCompilationTime;
    } else {
      totalCompilationTime = -1;
    }
    final GcObservation totalGcObservations = consolidate(startGcObservations, getGcObservations());
    System.out.format("%s[%s]: versions=%d; cells=%d, bytes=%,d, cpuTime=%s, userTime=%s, elapsedTime=%s," + " GC{count=%d, time=%s}, compileTime=%s, %,.2f cells/sec%n",
                      this.testName.getMethodName(),
                      keyType.getSimpleName(),
                      versionCount,
                      cellCount,
                      byteCount,
                      formatNanos(usedCpuTime),
                      formatNanos(usedUserTime),
                      formatNanos(elapsedTime),
                      totalGcObservations.collectionCount,
                      formatMillis(totalGcObservations.collectionTime),
                      (totalCompilationTime == -1 ? "n/a" : formatMillis(totalCompilationTime)),
                      (cellCount * 1.0E9d) / usedCpuTime);
  }

  private <K extends Comparable<K>, Z extends TimeReference<Z>> void generateRecords(final RecordGenerator<K, Z> generator) {

    /*
     * Limit record generation to 3/4 of available memory.  That should leave enough memory
     * available after record generation to process any single record.
     */
    final Runtime runtime = Runtime.getRuntime();
    final long maxMemory = runtime.maxMemory();
    final long reservedStorage = maxMemory - (maxMemory - (runtime.totalMemory() - runtime.freeMemory())) * 3 / 4;

    long versionCount = 0;
    int mostVersionCount = Integer.MIN_VALUE;
    int leastVersionCount = Integer.MAX_VALUE;
    long cellCount = 0;
    int mostCellCount = Integer.MIN_VALUE;
    int leastCellCount = Integer.MAX_VALUE;

    final long startNanos = System.nanoTime();
    for (int i = 0; i < MAX_RECORD_COUNT; i++) {
      if ((maxMemory - (runtime.totalMemory() - runtime.freeMemory())) <= reservedStorage) {
        System.out.format(
          "%s[%s]: Terminating record generation due to memory constraints [max=%sB, total=%sB, free=%sB, reserved=%sB]%n",
          this.testName.getMethodName(),
          generator.getKeyType().getSimpleName(),
          formatSize(maxMemory),
          formatSize(runtime.totalMemory()),
          formatSize(runtime.freeMemory()),
          formatSize(reservedStorage));
        break;
      }
      final VersionedRecord<K> record = generator.makeRecord(10);
      this.testRecords.add(record);

      final int recordVersionCount = record.elements().size();
      versionCount += recordVersionCount;
      mostVersionCount = Math.max(mostVersionCount, recordVersionCount);
      leastVersionCount = Math.min(leastVersionCount, recordVersionCount);

      int recordCellCount = 0;
      for (SovereignPersistentRecord<K> version : record.elements()) {
        recordCellCount += version.cells().size();
      }
      cellCount += recordCellCount;
      mostCellCount = Math.max(mostCellCount, recordCellCount);
      leastCellCount = Math.min(leastCellCount, recordCellCount);
    }

    /*
     * Attempt to ensure a clean memory environment before consuming the generated strings.
     */
    for (int i = 0; i < 10; i++) {
      runtime.gc();
      runtime.runFinalization();
      Thread.yield();
    }

    final long nanoDuration = System.nanoTime() - startNanos;
    System.out.format("%s[%s]: Record generation complete: %,d records (cells: total=%d, most=%d, least=%d;" + " versions: total=%d, most=%d, least=%d) generated in %s%n",
                      this.testName.getMethodName(),
                      generator.getKeyType().getSimpleName(),
                      this.testRecords.size(),
                      cellCount,
                      mostCellCount,
                      leastCellCount,
                      versionCount,
                      mostVersionCount,
                      leastVersionCount,
                      formatNanos(nanoDuration));
  }

  /**
   * Gets the {@code GarbageCollectorMXBean} details for each memory manager responsible for
   * {@link MemoryType#HEAP} memory.
   *
   * @return a map of the GC stats for each HEAP memory manager
   */
  private static Map<String, GcObservation> getGcObservations() {

    final Set<String> heapManagerNames = new HashSet<>();
    for (final MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
      if (bean.getType() == MemoryType.HEAP) {
        heapManagerNames.addAll(Arrays.asList(bean.getMemoryManagerNames()));
      }
    }
    final Map<String, GcObservation> gcObservations = new HashMap<>();
    for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (heapManagerNames.contains(bean.getName())) {
        gcObservations.putIfAbsent(bean.getName(), new GcObservation(bean));
      }
    }

    return gcObservations;
  }

  /**
   * Calculates the difference between the start and end garbage collection observations and
   * consolidates the differences into a single observation.
   *
   * @param start the starting {@code GcObservation} instances by memory manager
   * @param end the ending {@code GcObservation} instances by memory manager
   * @return a new {@code GcObservation} instances holding the sum of the observation differences
   */
  private static GcObservation consolidate(final Map<String, GcObservation> start,
                                           final Map<String, GcObservation> end) {

    long totalCollectionCount = 0;
    long totalCollectionTime = 0;
    for (final Map.Entry<String, GcObservation> endEntry : end.entrySet()) {
      final GcObservation endObservation = endEntry.getValue();
      final GcObservation startObservation = start.get(endEntry.getKey());
      if (startObservation == null) {
        // GC Manager added *after* starting observation
        totalCollectionCount += endObservation.collectionCount;
        totalCollectionTime += endObservation.collectionTime;
      } else {
        totalCollectionCount += endObservation.collectionCount - startObservation.collectionCount;
        totalCollectionTime += endObservation.collectionTime - startObservation.collectionTime;
      }
    }

    return new GcObservation(totalCollectionCount, totalCollectionTime);
  }

  // http://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
  private static String formatSize(long v) {
    if (v < 1024) {
      return v + " ";
    }
    int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
    return String.format("%.1f %si", (double) v / (1L << (z * 10)), " KMGTPE".charAt(z));
  }

  private static final class GcObservation {
    final long collectionCount;
    final long collectionTime;

    GcObservation(final long collectionCount, final long collectionTime) {
      this.collectionCount = collectionCount;
      this.collectionTime = collectionTime;
    }

    GcObservation(final GarbageCollectorMXBean gcBean) {
      this.collectionCount = gcBean.getCollectionCount();
      this.collectionTime = gcBean.getCollectionTime();
    }
  }

  private static final class Period {
    private final long duration;
    private final TimeUnit units;

    public Period(final long duration, final TimeUnit units) {
      this.duration = duration;
      this.units = units;
    }

    public long asMillis() {
      return this.units.toMillis(this.duration);
    }
  }

  /**
   * Runs all or selected methods from the {@code StringToolPerformanceTest} from the command line.
   *
   * @param args the argument list:
   * <ol>
   * <li>the number of times to repeat the selected tests</li>
   * <li>zero or more test method names</li>
   * </ol>
   */
  public static void main(String[] args) {
    System.setProperty(PERFORMANCE_TEST_PROPERTY, "true");
    new SelectedTestRunner(RecordBufferStrategyPerformanceTest.class, System.out).runTests(args);
  }
}
