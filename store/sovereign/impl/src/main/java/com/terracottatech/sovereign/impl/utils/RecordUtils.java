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
package com.terracottatech.sovereign.impl.utils;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.spi.store.PersistentRecord;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Simple classes to allow dumping record contents.
 *
 * @author cschanck
 */
public class RecordUtils {

  public static String dump(Record<?> rec, boolean includeVersions) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      dump(baos, rec, includeVersions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      return baos.toString("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Comparator<CellDefinition<?>> cellDefComp = new Comparator<CellDefinition<?>>() {
    @Override
    public int compare(CellDefinition<?> o1, CellDefinition<?> o2) {
      int ret = o1.name().compareTo(o2.name());
      if (ret == 0) {
        ret = o1.type().getJDKType().getTypeName().compareTo(o1.type().getJDKType().getTypeName());
      }
      return ret;
    }
  };

  private static Comparator<Cell<?>> cellComp = new Comparator<Cell<?>>() {
    @Override
    public int compare(Cell<?> o1, Cell<?> o2) {
      return cellDefComp.compare(o1.definition(), o2.definition());
    }
  };

  public static void dump(OutputStream os, Record<?> rec, boolean includeVersions) throws IOException {
    dumpSingle(os, rec);
    if (includeVersions && rec instanceof SovereignPersistentRecord<?>) {
      ((SovereignPersistentRecord<?>) rec).versions().forEach(r -> dumpSingle(os, r));
    }
    os.flush();
  }

  private static void dumpSingle(OutputStream os, Record<?> rec) {
    try (PrintWriter ps = new PrintWriter(new OutputStreamWriter(os, "UTF-8"))) {
      ps.print(valToString(rec.getKey()));
      if (rec instanceof PersistentRecord<?, ?>) {
        ps.println(" TR: " + ((PersistentRecord<?, ?>) rec).getTimeReference());
      }
      if (rec instanceof SovereignPersistentRecord) {
        ps.println(" MSN: " + ((SovereignPersistentRecord)rec).getMSN());
      } else {
        ps.println();
      }
      ArrayList<Cell<?>> list = new ArrayList<>();
      for (Cell<?> cell : rec) {
        list.add(cell);
      }
      Collections.sort(list, cellComp);
      for (Cell<?> c : list) {
        ps.println("  " + c.definition().name() + " :: " + valToString(c.value()));
      }
      ps.flush();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String valToString(Object key) {
    SovereignType t = SovereignType.forJDKType(key.getClass());
    switch (t) {
      case INT:
      case LONG:
      case DOUBLE:
      case CHAR:
      case STRING:
      case BOOLEAN:
        return "(" + t.name().toLowerCase() + ") '" + key.toString() + "'";
      case BYTES:
        return "(" + t.name().toLowerCase() + ") '" + stringBytes((byte[]) key, 20) + "'";
      default:
        throw new IllegalStateException();
    }
  }

  private static String stringBytes(byte[] key, int max) {
    StringBuilder sb = new StringBuilder();
    max = Math.min(key.length, max);
    sb.append("[").append(key.length).append("] ");
    boolean first = true;
    for (int i = 0; i < max; i++) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(key[i]);
    }
    if (max != key.length) {
      sb.append("...");
    }
    return sb.toString();
  }
}
