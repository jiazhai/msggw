package io.streaml.msggw.kafka;

/*
 * Copyright 2019 Streamlio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.Test;


public class MessageIdUtilsTest {
    @Test
    public void testNoBatch() {
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(1, 0, -1, -1);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));
    }

    @Test
    public void testBatch() {
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(1, 0, -1, 10);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));

        msgId = new BatchMessageIdImpl(1, 0, -1, 0);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));

        msgId = new BatchMessageIdImpl(1, 0, -1, (0xFF-1));
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testBatchTooHigh() {
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(1, 0, -1, 0xFFF);
        MessageIdUtils.getOffset(msgId);
    }

    @Test
    public void testPartition() {
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(1, 0, 0, 0);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));

        msgId = new BatchMessageIdImpl(1, 0, 10, 0);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));

        msgId = new BatchMessageIdImpl(1, 0, (0x3F - 1), 0);
        assertThat(MessageIdUtils.getMessageId(MessageIdUtils.getOffset(msgId)),
                   equalTo(msgId));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testPartitionTooHigh() {
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(1, 0, 0x3F, 0);
        MessageIdUtils.getOffset(msgId);
    }

}
