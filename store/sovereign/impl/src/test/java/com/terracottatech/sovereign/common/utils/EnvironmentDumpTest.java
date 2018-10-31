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

import org.junit.Test;
import org.terracotta.offheapstore.util.PhysicalMemory;

import java.io.PrintStream;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author cschanck
 **/
public class EnvironmentDumpTest {

  @Test
  public void testDump() {
    System.out.println("=== JVM Environment Dump ===");
    dumpMemAndHW(System.out);
    dumpSysProperties(System.out);
    dumpEnvVar(System.out);
  }

  private void dumpMemAndHW(PrintStream out) {
    Runtime r = Runtime.getRuntime();
    out.println("Runtime Memory and Hardware");
    out.println("     Processors: " + r.availableProcessors());
    out.println("     Max Memory: " + MiscUtils.bytesAsNiceString(r.maxMemory()));
    out.println("    Free Memory: " + MiscUtils.bytesAsNiceString(r.freeMemory()));
    out.println("   Total Memory: " + MiscUtils.bytesAsNiceString(r.totalMemory()));
    out.println("Management Memory");
    out.println("   Machine Free Physical Memory: " + MiscUtils.bytesAsNiceString(PhysicalMemory.freePhysicalMemory()));
    out.println("        Machine Free Swap Space: " + MiscUtils.bytesAsNiceString(PhysicalMemory.freeSwapSpace()));
    out.println("       Process Committed Memory: " + MiscUtils.bytesAsNiceString(PhysicalMemory.ourCommittedVirtualMemory()));
    out.println("  Machine Total Physical Memory: " + MiscUtils.bytesAsNiceString(PhysicalMemory.totalPhysicalMemory()));
    out.println("       Machine Total Swap Space: " + MiscUtils.bytesAsNiceString(PhysicalMemory.totalSwapSpace()));
  }

  private void dumpSysProperties(PrintStream out) {
    out.println("System Properties");
    Properties props = System.getProperties();
    TreeMap<String, String> sorted = new TreeMap<>();
    for (Object k : props.keySet()) {
      sorted.put((String) k, (String) props.get(k));
    }
    for (String k : sorted.keySet()) {
      out.println("   " + k + " :: [" + sorted.get(k) + "]");
    }
    out.println(sorted);
  }

  private void dumpEnvVar(PrintStream out) {
    out.println("System Environment");
    TreeMap<String, String> sorted = new TreeMap<>(System.getenv());
    for (String k : sorted.keySet()) {
      out.println("   " + k + " :: [" + sorted.get(k) + "]");
    }
  }
}
