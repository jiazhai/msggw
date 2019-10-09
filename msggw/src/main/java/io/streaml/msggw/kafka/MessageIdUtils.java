/**
 * Copyright 2019 Streamlio, Inc.
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streaml.msggw.kafka;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoding from pulsar msgid to kafka offset.
 * Encoded as:
 * |Ledger ID||Entry ID ||Partition||Batch Idx|
 * [ 24 bits ][ 22 bits ][ 6 bits  ][ 12 bits ]
 * As such, there is a maximum value for each.
 * Max ledger ID = 16777215
 * Max entry ID = 4194303
 * Max partition = 63
 * Max batch index = 4095
 *
 * Copied from Apache Pulsar, and modified at lot since.
 * Path: pulsar-client-kafka-compat/pulsar-client-kafka/src/main/java/org/apache/pulsar/client/kafka/compat/MessageIdUtils.java 
 * Commit-sha: e7f1160f563e433f1bb2ec70da827c7d420c23d2
 * Url: https://github.com/apache/pulsar/blob/e7f1160f563e433f1bb2ec70da827c7d420c23d2/pulsar-client-kafka-compat/pulsar-client-kafka/src/main/java/org/apache/pulsar/client/kafka/compat/MessageIdUtils.java
 */
public class MessageIdUtils {
    private static final Logger log = LoggerFactory.getLogger(MessageIdUtils.class);
    private static final long LEDGER_MASK = 0xFF_FF_FF;
    private static final long LEDGER_SHIFT = 40;
    private static final long ENTRY_MASK = 0x3F_FF_FF;
    private static final long ENTRY_SHIFT = 18;
    private static final long PARTITION_MASK = 0x3F;
    private static final long PARTITION_SHIFT = 12;
    private static final long BATCH_MASK = 0xF_FF;
    private static final long BATCH_SHIFT = 0;

    public static final long getOffset(MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();
        long partitionId = msgId.getPartitionIndex();
        long batchId = -1;
        if (messageId instanceof BatchMessageIdImpl) {
            batchId = ((BatchMessageIdImpl)msgId).getBatchIndex();
        }
        return getOffset(ledgerId, entryId, partitionId, batchId);
    }

    public static final long getOffset(long ledgerId, long entryId) {
        return getOffset(ledgerId, entryId, -1, -1);
    }

    public static final long getOffset(long ledgerId, long entryId, long partitionId, long batchId) {
        if (ledgerId >= LEDGER_MASK) {
            throw new IllegalArgumentException(
                    String.format("Ledger id (%d) larger than highest usable ledger id (%d) for kafka gw",
                                  ledgerId, LEDGER_MASK - 1));
        }
        if (entryId >= ENTRY_MASK) {
            throw new IllegalArgumentException(
                    String.format("Entry id (%d) larger than highest usable entry id (%d) for kafka gw",
                            entryId, ENTRY_MASK - 1));
        }
        if (partitionId >= PARTITION_MASK) {
            throw new IllegalArgumentException(
                    String.format("Partition id (%d) larger than highest usable partition id (%d) for kafka gw",
                                  partitionId, PARTITION_MASK - 1));
        }
        if (batchId >= BATCH_MASK) {
            throw new IllegalArgumentException(
                    String.format("Batch id (%d) larger than highest usable batch id (%d) for kafka gw",
                                  batchId, BATCH_MASK - 1));
        }
        return ((ledgerId & LEDGER_MASK) << LEDGER_SHIFT)
            | ((entryId & ENTRY_MASK) << ENTRY_SHIFT)
            | (((partitionId + 1) & PARTITION_MASK) << PARTITION_SHIFT)
            | (((batchId + 1) & BATCH_MASK) << BATCH_SHIFT);
    }

    public static final MessageId getMessageId(long offset) {
        long ledgerId = (offset >> LEDGER_SHIFT) & LEDGER_MASK;
        long entryId = (offset >> ENTRY_SHIFT) & ENTRY_MASK;
        int partitionId = (int)((offset >> PARTITION_SHIFT) & PARTITION_MASK) - 1;
        int batchId = (int)((offset >> BATCH_SHIFT) & BATCH_MASK) - 1;
        return new BatchMessageIdImpl(ledgerId, entryId, partitionId, batchId);
    }
}
