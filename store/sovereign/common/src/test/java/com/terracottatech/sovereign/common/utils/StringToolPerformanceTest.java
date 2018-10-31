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

package com.terracottatech.sovereign.common.utils;

import com.terracottatech.sovereign.testsupport.TestUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.terracottatech.sovereign.testsupport.SelectedTestRunner;

import java.io.UTFDataFormatException;
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
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Runs performance (timing) tests over {@link StringTool}.  This class includes a
 * {@link #main(String[])} that may be used to run all or selected tests repeatedly.
 *
 * @author Clifford W. Johnson
 */
public class StringToolPerformanceTest {

  private static final Period TEST_DURATION = new Period(5, TimeUnit.MINUTES);
  private static final int MAX_STRING_LENGTH = 4 * 1024 * 1024;
  private static final int MAX_STRING_COUNT = 1500;

  /**
   * When this system property is set to {@code true}, the performance tests are run.
   */
  public static final String PERFORMANCE_TEST_PROPERTY = "com.terracottatech.sovereign.stringTool.performance.test";

  /**
   * Reference to {@link ThreadMXBean}.  This reference is used to obtain statistics about thread performance.
   */
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
  static {
    THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
  }

  private static final CompilationMXBean COMPILATION_MX_BEAN = ManagementFactory.getCompilationMXBean();

  @Rule
  public TestName testName = new TestName();

  private final String runPerformanceTests = System.getProperty(PERFORMANCE_TEST_PROPERTY, "false");

  private final ByteBuffer buffer = ByteBuffer.allocate(MAX_STRING_LENGTH * 3 + 9);

  private final List<String> testStrings = new ArrayList<>(MAX_STRING_COUNT);

  private Random rnd;
  private volatile boolean testComplete;
  private long byteCount;
  private Map<Integer, Integer> typeCountMap;

  @Before
  public void setUp() throws Exception {
    this.rnd = new Random(8675309L);
  }

  @After
  public void cleanUp() {
    this.testStrings.clear();
    final Runtime runtime = Runtime.getRuntime();
    for (int i = 0; i < 10; i++) {
      runtime.gc();
      runtime.runFinalization();
      Thread.yield();
    }
  }

  @Test
  public void testAsciiOnlySizing() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.RANDOM));
    this.timeOperation(this::sizeOperation);
  }

  @Test
  public void testAsciiOnlyEncode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void testAsciiOnlyEncodeDecode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void testAsciiOnlyEncodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void testAsciiOnlyEncodeDecodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void testAsciiOnlyEncodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void testAsciiOnlyEncodeDecodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void testAsciiOnlyEncodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void testAsciiOnlyEncodeDecodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(-0x007F, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test2ByteStringSizing() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.RANDOM));
    this.timeOperation(this::sizeOperation);
  }

  @Test
  public void test2ByteStringEncode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test2ByteStringEncodeDecode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test2ByteStringEncodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test2ByteStringEncodeDecodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test2ByteStringEncodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test2ByteStringEncodeDecodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test2ByteStringEncodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test2ByteStringEncodeDecodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(0x07FF, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test3ByteStringSizing() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.RANDOM));
    this.timeOperation(this::sizeOperation);
  }

  @Test
  public void test3ByteStringEncode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test3ByteStringEncodeDecode() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.RANDOM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test3ByteStringEncodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test3ByteStringEncodeDecodeSmall() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.SMALL));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test3ByteStringEncodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test3ByteStringEncodeDecodeMedium() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.MEDIUM));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  @Test
  public void test3ByteStringEncodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, false));
  }

  @Test
  public void test3ByteStringEncodeDecodeLarge() throws Exception {
    assumeThat(Boolean.valueOf(this.runPerformanceTests), is(true));
    this.generateStrings(() -> this.randomString(Character.MAX_CODE_POINT, SizeRange.LARGE));
    this.timeOperation((s) -> encodeDecodeOperation(s, true));
  }

  /**
   * Executes the operation provided over a pseudo-random sequence of strings ({@link #testStrings})
   * for the time designated by {@link #TEST_DURATION}.
   *
   * @param operation the {@code Consumer} specifying the operation to test
   */
  private void timeOperation(final Consumer<String> operation) {
    long stringCount = 0;
    long charCount = 0;
    this.byteCount = 0;
    this.typeCountMap = new HashMap<>();

    this.testComplete = false;
    final Timer timer = new Timer("TestTimer", true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        StringToolPerformanceTest.this.testComplete = true;
      }
    }, TEST_DURATION.asMillis());

    final Map<String, GcObservation> startGcObservations = getGcObservations();
    final long startCompilationTime;
    if (COMPILATION_MX_BEAN.isCompilationTimeMonitoringSupported()) {
      startCompilationTime = COMPILATION_MX_BEAN.getTotalCompilationTime();
    } else {
      startCompilationTime = -1;
    }
    final long threadId = Thread.currentThread().getId();
    final long startElapsedTime = System.nanoTime();
    final long startCpuTime = THREAD_MX_BEAN.getThreadCpuTime(threadId);
    final long startUserTime = THREAD_MX_BEAN.getThreadUserTime(threadId);

    while (!this.testComplete) {
      final String testString = this.testStrings.get(this.rnd.nextInt(this.testStrings.size()));
      stringCount++;
      charCount += testString.length();
      operation.accept(testString);
    }

    final long usedUserTime = THREAD_MX_BEAN.getThreadUserTime(threadId) - startUserTime;
    final long usedCpuTime = THREAD_MX_BEAN.getThreadCpuTime(threadId) - startCpuTime;
    final long elapsedTime = System.nanoTime() - startElapsedTime;
    final long totalCompilationTime;
    if (COMPILATION_MX_BEAN.isCompilationTimeMonitoringSupported()) {
      totalCompilationTime = COMPILATION_MX_BEAN.getTotalCompilationTime() - startCompilationTime;
    } else {
      totalCompilationTime = -1;
    }
    final GcObservation totalGcObservations = consolidate(startGcObservations, getGcObservations());
    System.out.format(
      "%s: stringCount=%,d, typeCounts=%s, charCount=%,d, bytes=%,d, cpuTime=%s, userTime=%s, elapsedTime=%s, GC{count=%d, time=%s}, compileTime=%s, %,.2f cps%n",
      this.testName.getMethodName(), stringCount, this.typeCountMap, charCount, this.byteCount,
      TestUtility.formatNanos(usedCpuTime), TestUtility.formatNanos(usedUserTime), TestUtility.formatNanos(elapsedTime),
      totalGcObservations.collectionCount, TestUtility.formatMillis(totalGcObservations.collectionTime),
      (totalCompilationTime == -1 ? "n/a" : TestUtility.formatMillis(totalCompilationTime)),
      (charCount * 1.0E9d) / usedCpuTime);
  }

  /**
   * Exercises the {@link StringTool#getLengthAsUTF(String)} method.
   *
   * @param testString the string to use for the test
   */
  private void sizeOperation(final String testString) {
    this.byteCount += StringTool.getLengthAsUTF(testString);
  }

  /**
   * Exercises the {@link StringTool#putUTF(ByteBuffer, String)} method and, optionally, the
   * {@link StringTool#getUTF(ByteBuffer)} method.
   *
   * @param testString the string to use for the test
   * @param decode if {@code true}, {@code StringTool.getUTF(ByteBuffer)} is called after
   *               {@code StringTool.putUTF(ByteBuffer, String)}
   */
  private void encodeDecodeOperation(final String testString, final boolean decode) {
    try {
      this.buffer.clear();
      StringTool.putUTF(this.buffer, testString);
      this.byteCount += this.buffer.position();
      this.buffer.flip();
      final int encodingType = Byte.toUnsignedInt(this.buffer.get());
      Integer typeCount = this.typeCountMap.get(encodingType);
      if (typeCount == null) {
        typeCount = 0;
      }
      typeCount++;
      this.typeCountMap.put(encodingType, typeCount);
      if (decode) {
        this.buffer.rewind();
        StringTool.getUTF(this.buffer);
      }
    } catch (UTFDataFormatException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Generates up to {@link #MAX_STRING_COUNT} pseudo-random strings limited by memory usage.
   *
   * @param generator the string generator to use
   */
  private void generateStrings(final Supplier<String> generator) {
    /*
     * Limit string generation to 3/4 of available memory.  That should leave enough memory
     * available after string generation to process any single string.
     */
    final Runtime runtime = Runtime.getRuntime();
    final long maxMemory = runtime.maxMemory();
    final long reservedStorage = maxMemory - (maxMemory - (runtime.totalMemory() - runtime.freeMemory())) * 3 / 4;

    long charCount = 0;
    long largestStringSize = Long.MIN_VALUE;
    long smallestStringSize = Long.MAX_VALUE;

    final long startNanos = System.nanoTime();
    for (int i = 0; i < MAX_STRING_COUNT; i++) {
      if ((maxMemory - (runtime.totalMemory() - runtime.freeMemory())) <= reservedStorage) {
        System.out.format("%s: Terminating string generation due to memory constraints [max=%sB, total=%sB, free=%sB, reserved=%sB]%n",
            this.testName.getMethodName(),
            formatSize(maxMemory), formatSize(runtime.totalMemory()), formatSize(runtime.freeMemory()), formatSize(reservedStorage));
        break;
      }
      final String string = generator.get();
      this.testStrings.add(string);
      charCount += string.length();
      largestStringSize = Math.max(largestStringSize, string.length());
      smallestStringSize = Math.min(smallestStringSize, string.length());
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
    System.out.format("%s: String generation complete: %,d strings (characters: total=%,d, largest=%,d, smallest=%,d) generated in %s%n",
                      this.testName.getMethodName(), this.testStrings.size(), charCount, largestStringSize, smallestStringSize, TestUtility
                        .formatNanos(nanoDuration));
  }

  private String randomString(int maxCodePoint, final SizeRange sizeRange) {
    boolean no0x00 = false;
    if (maxCodePoint < 0) {
      no0x00 = true;
      maxCodePoint = -maxCodePoint;
    }
    maxCodePoint++;

    final int maxStringLength;
    switch (sizeRange) {
      case SMALL:
        maxStringLength = this.rnd.nextInt(2048);
        break;
      case MEDIUM:
        maxStringLength = 65536 + this.rnd.nextInt(65536);
        break;
      case LARGE:
        maxStringLength = 65536 + this.rnd.nextInt(MAX_STRING_LENGTH);
        break;
      default:
        if (this.rnd.nextBoolean()) {
          maxStringLength = this.rnd.nextInt(1024);
        } else {
          maxStringLength = this.rnd.nextInt(MAX_STRING_LENGTH);
        }
        break;
    }

    final StringBuilder sb = new StringBuilder(maxStringLength);
    int remaining;
    while ((remaining = maxStringLength - sb.length()) > 0) {
      int codePoint;
      do {
        codePoint = this.rnd.nextInt(maxCodePoint);
        if (no0x00) {
          while (codePoint == 0) {
            codePoint = this.rnd.nextInt(maxCodePoint);
          }
        }
      } while (Character.charCount(codePoint) > remaining);
      sb.appendCodePoint(codePoint);
    }

    return sb.toString();
  }

  private enum SizeRange {
    RANDOM,
    SMALL,
    MEDIUM,
    LARGE
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
   *
   * @return a new {@code GcObservation} instances holding the sum of the observation differences
   */
  private static GcObservation consolidate(final Map<String, GcObservation> start, final Map<String, GcObservation> end) {

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

  // http://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
  private static String formatSize(long v) {
    if (v < 1024) return v + " ";
    int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
    return String.format("%.1f %si", (double) v / (1L << (z * 10)), " KMGTPE".charAt(z));
  }

  /**
   * Runs all or selected methods from the {@code StringToolPerformanceTest} from the command line.
   *
   * @param args the argument list:
   * <ol>
   *   <li>the number of times to repeat the selected tests</li>
   *   <li>zero or more test method names</li>
   * </ol>
   */
  public static void main(String[] args) {
    System.setProperty(PERFORMANCE_TEST_PROPERTY, "true");
    new SelectedTestRunner(StringToolPerformanceTest.class, System.out).runTests(args);
  }
}
