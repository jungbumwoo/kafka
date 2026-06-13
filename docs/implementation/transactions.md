---
title: Transactions
description: Kafka transactions overview
weight: 5
tags: ['kafka', 'docs', 'transactions']
aliases:
keywords:
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Kafka transactions let a producer publish records to one or more partitions atomically. A transaction can also include consumer offsets, which allows a consume-process-produce loop to commit its output records and its input progress together.

## Transaction lifecycle

The common producer flow is:

1. `initTransactions()`
2. `beginTransaction()`
3. `send(...)`
4. `sendOffsetsToTransaction(...)` when the application also consumes input
5. `commitTransaction()` or `abortTransaction()`

`initTransactions()` establishes ownership of the configured `transactional.id`. If an earlier producer instance with the same id left an unfinished transaction behind, Kafka resolves that state before the new producer starts.

`beginTransaction()` opens a new transactional boundary. All writes after this call stay invisible to `read_committed` consumers until `commitTransaction()` succeeds.

`sendOffsetsToTransaction(...)` is the key step for exactly-once consume-process-produce pipelines. The offsets recorded here represent the next input positions to resume from after the transaction commits. If the transaction aborts, neither the produced records nor those offsets become visible.

`abortTransaction()` rolls back the in-flight transaction. Applications typically pair that with rewinding the consumer to the last committed offsets so the same input can be processed again.

## Consumer visibility

Consumers with `isolation.level=read_committed` only return records from committed transactions. Records written in aborted transactions remain in the log for replication and recovery purposes, but they are filtered out for those consumers.

Consumers with `isolation.level=read_uncommitted` can observe both committed and aborted transactional writes.

## Example in this repository

`tools/src/main/java/org/apache/kafka/tools/TransactionalMessageCopier.java` demonstrates a transactional consume-process-produce loop:

1. Poll input records from a consumer configured with `read_committed`.
2. Begin a producer transaction.
3. Write transformed output records.
4. Send the consumer offsets into the same transaction.
5. Commit on success, or abort and rewind the consumer on failure.

`clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/TransactionsWithMaxInFlightOneTest.java` demonstrates the visibility rule by writing one aborted transaction and one committed transaction, then verifying that a `read_committed` consumer only sees the committed records.
