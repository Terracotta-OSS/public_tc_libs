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

package com.terracottatech.store.transactions.impl;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Optional;
import java.util.UUID;

import static com.terracottatech.store.transactions.impl.TransactionImpl.TRANSACTIONAL_CELLNAME_PREFIX;

class TransactionID {
  private final UUID transactionID;
  private static final String TRANSACTION_ID_CELLNAME = TRANSACTIONAL_CELLNAME_PREFIX + "id";
  private static final StringCellDefinition TRANSACTION_ID = CellDefinition.defineString(TRANSACTION_ID_CELLNAME);

  TransactionID() {
    transactionID = UUID.randomUUID();
  }

  private TransactionID(UUID transactionID) {
    this.transactionID = transactionID;
  }

  @Override
  public String toString() {
    return transactionID.toString();
  }

  static <K extends Comparable<K>> Optional<TransactionID> getTransactionID(Record<K> record) {
    return record.get(TRANSACTION_ID).map(string -> new TransactionID(UUID.fromString(string)));
  }


  static TransactionID getTransactionID(String stringTransactionID) {
    return new TransactionID(UUID.fromString(stringTransactionID));
  }

  static boolean isTransactionIDCell(Cell<?> cell) {
    return cell.definition().equals(TRANSACTION_ID);
  }

  Cell<?> cell() {
    return TRANSACTION_ID.newCell(toString());
  }

  String getSystemTransactionsKeyValue() {
    return toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionID that = (TransactionID) o;

    return transactionID.equals(that.transactionID);
  }

  @Override
  public int hashCode() {
    return transactionID.hashCode();
  }
}
