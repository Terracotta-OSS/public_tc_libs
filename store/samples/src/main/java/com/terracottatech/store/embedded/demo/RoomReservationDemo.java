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
package com.terracottatech.store.embedded.demo;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Optional;

import static com.terracottatech.store.UpdateOperation.output;
import static com.terracottatech.store.UpdateOperation.write;

/**
 * Simple room reservation demo.
 */
public class RoomReservationDemo {

  public static void main(String[] args) throws StoreException {
    DatasetManager datasetManager = DatasetManager.embedded().offheap("offheap", 128L, MemoryUnit.MB).build(); // <1>

    DatasetConfiguration embeddedConfig =
        datasetManager.datasetConfiguration().offheap("offheap").build(); // <2>

    datasetManager.newDataset("rooms", Type.INT, embeddedConfig);

    try (Dataset<Integer> rooms = datasetManager.getDataset("rooms", Type.INT)) { // <3>
      DatasetWriterReader<Integer> access = rooms.writerReader();

      BoolCellDefinition available = CellDefinition.defineBool("available");
      IntCellDefinition rent = CellDefinition.defineInt("rent");
      StringCellDefinition reservedBy = CellDefinition.defineString("reservedBy");

      // Create some rooms in the hotel
      for (int roomNumber = 0; roomNumber < 10; roomNumber++) {
        access.add(roomNumber, available.newCell(true), rent.newCell(1000));
      }

      // Try to book one of the rooms
      Optional<String> owner = access.on(5)
          .iff(available.isTrue())
          .update(UpdateOperation.allOf(write(available).value(false), write(reservedBy).value("Albin")),
              output(reservedBy.valueOrFail()));

      owner.ifPresent(name -> System.out.println("Reserved by " + name));
    }
  }
}
