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

SystemTransactions
===============
1) Transaction-id will be key and will have following cells in the record
   - BeginTime => in milliseconds  (only for diagnostic purpose)
   - Deadline   => in milliseconds
   - Status    => EXECUTING, COMMITTED, ROLLEDBACK
2) Should be persistent Dataset if using for transactions on persistent datasets, this is to know the status of
   transaction for dirty records after a reboot. This will not be forced but will be documented.
3) If a transaction want to CUD a record which is uncommited by another transaction say T, then it would check if T
   has been marked as committed/rolledback/timeout using T's systemtransaction's record and try to resolve that record
   accordingly.

Begin transaction
=================
1) Generate transaction-id
    - Transaction-id would be RandomUUID.toString(), if insert into SystemTransactions fail,
      then generate another transaction-id
2) Add SystemTransactions record
    - generated txid as key
    - beginTime => current time
    - Deadline => current time + provided timeout
    - Status => EXECUTING

Add record
==========
1) Add record with following cells:
    - record cells with original cell names prefixed with UNCOMMITED_CELL_PREFIX (TRANSACTIONAL_CELLNAME_PREFIX + "pending:")
      Here TRANSACTIONAL_CELLNAME_PREFIX is "__tc_store_tx:".
    - Cell to keep the transaction-id with cell name as TRANSACTION_ID_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + "id")
    - Cell to store the operation type information with cell name as TRANSACTIONAL_OPERATION_TYPE_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + "operation") and value as "ADD"
2) If committed record already present then return false.
3) If a uncommited record with the same key is already present, then try to resolve the record or wait for that
   record to be committed/rollbacked. Then try again.
4) If uncommitted record with same key is already present but has been dirtied by self then:
    - Deleted Record: keep the pre-deleted image in the record, add new cells with names prefixed with
      UNCOMMITED_CELL_PREFIX. Operation_cell would say UPDATE, transactionID cell would remain unchanged.
    - Updated Record: return false
    - Added Record: return false

Delete record
=============
1) Keep the record intact and only add the following new cells to logically delete the record:
    - Cell to keep the transaction-id with cell name as TRANSACTION_ID_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + TRANSACTION_ID)
    - Cell to store the operation type information with cell name as TRANSACTIONAL_OPERATION_TYPE_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + OPERATION_TYPE) and value as "DELETE"
2) If record not present return false
3) If a uncommited record with the same key is already present, then try to resolve the record or wait for that
   record to be committed/rollbacked. Then retry again.
4) If uncommitted record with same key is already present but has been dirtied by self then:
    - Deleted Record: return false
    - Updated Record: remove cells added by update operation and add new cells as if the record is freshly being deleted
    - Added Record: delete the whole record (NOTE: delete means actual removal, not marking as deleted)

Update record
=============
1) Keep the existing cells on the record and add following new cells to logically update the record:
    - new cells with cell names prefixed with UNCOMMITED_CELL_PREFIX (TRANSACTIONAL_CELLNAME_PREFIX)
    - Cell to keep the transaction-id with cell name as TRANSACTION_ID_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + TRANSACTION_ID)
    - Cell to store the operation type information with cell name as TRANSACTIONAL_OPERATION_TYPE_CELLNAME
      (TRANSACTIONAL_CELLNAME_PREFIX + OPERATION_TYPE) and value as "UPDATE"
2) If record not present return false
3) If a uncommited record with the same key is already present, then try to resolve the record or wait for that
   record to be committed/rollbacked. Then retry again.
4) If uncommitted record with same key is already present but has been dirtied by self then:
    - Deleted Record: return false
    - Updated Record: remove cells added by previous update operation and add new cells as if the record is freshly
      being updated
    - Added Record: remove already existing cells and add new cells as if the record has been newly added

Get record
==========
1) Always gets the last committed record image and hence is a non-blocking operation unlike CUD operations
2) If a record uncommitted by other transaction is encountered, then check that transaction's status:
    - EXECUTING : return previously committed image of record if not an added record, if it is an added record, return false
    - ROLLEDBACK : return previously committed image of record, but leave the uncommitted record in its
      present state. Actually, here we do not have a writerReader to cleanup the record hence leaving it intact.
    - COMMITTED: return newer image of record and leave the record intact.
3) If uncommitted by self, then return latest image of record.

Commit transaction
==================
1) SystemTransactions.Status => COMMITTED
2) Post-Commit: Resolve every record CUDed by this transaction
3) Delete the SystemTransaction's record corresponding to this transaction

Rollback transaction
====================
1) SystemTransactions.Status => ROLLEDBACK
2) Post-Rollback: Resolve every record CUDed by this transaction by rolling back the changes
3) Delete the SystemTransaction's record corresponding to this transaction

Other Notes
===========
- If a client gets disconnected or closes the connection to the server, all the open transaction of that client
  would be left to eventually time out.