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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("serial")
public class PersistableSchemaList implements Externalizable {

  private final ArrayList<SchemaCellDefinition<?>> snap = new ArrayList<>();

  public PersistableSchemaList() {
  }

  public PersistableSchemaList(Collection<SchemaCellDefinition<?>> schemaCellDefinitions) {
    snap.addAll(schemaCellDefinitions);
    Collections.sort(snap);
  }

  PersistableSchemaList(DatasetSchemaBackend backend) {
    this(backend.getContents().values());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(snap.size());
    for (SchemaCellDefinition<?> scd : snap) {
      scd.writeTo(out);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    snap.clear();
    int many = in.readInt();
    for (int i = 0; i < many; i++) {
      SchemaCellDefinition<?> impl = SchemaCellDefinition.readExternal(in);
      snap.add(impl);
    }
    Collections.sort(snap);
  }

  public List<SchemaCellDefinition<?>> getDefinitions() {
    return Collections.unmodifiableList(snap);
  }
}
