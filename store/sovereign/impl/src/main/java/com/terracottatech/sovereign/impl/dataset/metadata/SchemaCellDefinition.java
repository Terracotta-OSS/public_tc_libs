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
package com.terracottatech.sovereign.impl.dataset.metadata;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author cschanck
 **/
public class SchemaCellDefinition<T> implements Comparable<SchemaCellDefinition<?>> {

  private final CellDefinition<T> def;
  private final int id;
  private final SovereignType stype;

  public SchemaCellDefinition(CellDefinition<T> def, int id) {
    this.def = def;
    this.id = id;
    this.stype = SovereignType.forJDKType(def.type().getJDKType());
  }

  public SchemaCellDefinition(CellDefinition<T> def, SovereignType t, int id) {
    this.def = def;
    this.id = id;
    this.stype = t;
  }

  public CellDefinition<T> definition() {
    return def;
  }

  public int id() {
    return id;
  }

  public int compareTo(SchemaCellDefinition<?> o) {
    int ret = def.name().compareTo(o.definition().name());
    if (ret == 0) {
      ret = Integer.compare(id, o.id());
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaCellDefinition<?> that = (SchemaCellDefinition<?>) o;

    if (id != that.id) {
      return false;
    }
    return def.equals(that.def);

  }

  @Override
  public int hashCode() {
    int result = def.hashCode();
    result = 31 * result + id;
    return result;
  }

  public void writeTo(ObjectOutput out) throws IOException {
    out.writeInt(id);
    out.writeShort(stype.ordinal());
    out.writeUTF(def.name());
  }

  public static SchemaCellDefinition<?> readExternal(ObjectInput in) throws IOException {
    int id = in.readInt();
    short ord = in.readShort();
    SovereignType stype = SovereignType.values()[ord];
    Type<?> type = stype.getNevadaType();
    String nm = in.readUTF();
    return new SchemaCellDefinition<>(CellDefinition.define(nm, type), stype, id);
  }
}



