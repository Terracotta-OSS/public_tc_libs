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

package com.terracottatech.store;

import com.terracottatech.store.definition.CellDefinition;
import org.junit.Test;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.UpdateOperation.allOf;
import static com.terracottatech.store.UpdateOperation.remove;
import static com.terracottatech.store.UpdateOperation.write;
import static java.util.Optional.of;
import static org.mockito.Mockito.mock;

public class MutationTest {
    private static CellDefinition<String> NAME1 = defineString("name1");
    private static CellDefinition<String> NAME2 = defineString("name2");

    private Dataset<String> dataset = dataset();
  private DatasetReader<String> datasetReader = dataset.reader();
    private DatasetWriterReader<String> datasetWriterReader = dataset.writerReader();

    ////////////////////////
    //
    // Create
    //
    ////////////////////////

    @Test
    public void addBasic() {
        boolean created = datasetWriterReader.add("key1",
                NAME1.newCell("value1"),
                NAME2.newCell("value2")
        );
    }

    @Test
    public void addOutputs() {
        Optional<Record<String>> existingRecord = datasetWriterReader.on("key1").add();

        Optional<String> cellValue = datasetWriterReader.on("key1").add(NAME1.value(), NAME1.newCell("value")).orElse(of("value"));
    }

    ////////////////////////
    //
    // Read
    //
    ////////////////////////

    @Test
    public void readBasic() {
        Optional<Record<String>> record1 = datasetReader.get("key1");
        Optional<Record<String>> record2 = datasetWriterReader.get("key1");
    }

    @Test
    public void readerWhere() {
        Optional<Record<String>> record1 = datasetReader.on("key1").iff(NAME1.exists()).read();
        Optional<Record<String>> record2 = datasetWriterReader.on("key1").iff(NAME1.exists()).read();
    }

    @Test
    public void readerOutputs() {
        Optional<Optional<String>> value1 = datasetReader.on("key1")
                .read(NAME2.value());

        Optional<Optional<String>> value2 = datasetWriterReader.on("key1")
                .read(NAME2.value());
    }

    ////////////////////////
    //
    // Update
    //
    ////////////////////////

    @Test
    public void updateBasic() {
        datasetWriterReader.update("key1", write(NAME1.newCell("newValue1")));
    }

    @Test
    public void updateMultiple() {
        datasetWriterReader.update("key1", allOf(
                write(NAME1).value("newValue1"),
                write(NAME2).value("newValue2")
        ));
    }

    @Test
    public void updaterWhere() {
        datasetWriterReader.on("key1")
                .iff(NAME1.exists())
                .update(write(NAME1).value("newValue"));
    }

    @Test
    public void updaterOutputs() {
        Optional<Record<String>> beforeRecord = datasetWriterReader.on("key1").update(remove(NAME1), outputBeforeRecord());
        Optional<Record<String>> afterRecord = datasetWriterReader.on("key1").update(remove(NAME1), outputAfterRecord());
        boolean cellChanged = datasetWriterReader.on("key1").update(write(NAME1).value("new"), outputCellChanged(NAME1)).orElse(false);
    }

    ////////////////////////
    //
    // Delete
    //
    ////////////////////////

    @Test
    public void deleteBasic() {
        datasetWriterReader.delete("key1");
    }

    @Test
    public void deleteWhere() {
        datasetWriterReader.on("key1").iff(NAME1.exists()).delete();
    }

    @Test
    public void deleteFiltering() {
        Optional<Optional<String>> oldValue = datasetWriterReader.on("key1").delete(NAME1.value());
    }

    ////////////////////////
    //
    // Helpers
    //
    ////////////////////////

    private Dataset<String> dataset() {
        return new TestDataset<>();
    }

    @SuppressWarnings("unchecked")
    private BiFunction<Record<String>, Record<String>, Record<String>> outputBeforeRecord() {
        return mock(BiFunction.class);
    }

    @SuppressWarnings("unchecked")
    private BiFunction<Record<String>, Record<String>, Record<String>> outputAfterRecord() {
        return mock(BiFunction.class);
    }

    @SuppressWarnings("unchecked")
    private BiFunction<Record<String>, Record<String>, Boolean> outputCellChanged(CellDefinition<?> cellDefinition) {
        return mock(BiFunction.class);
    }
}
