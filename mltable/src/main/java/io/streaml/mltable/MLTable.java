package io.streaml.mltable;

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

import java.util.concurrent.CompletableFuture;

public interface MLTable extends AutoCloseable {
    public static final long NEW = -19731;
    public static final long ANY = -93730;

    public static MLTableBuilder newBuilder() {
        return new MLTableBuilderImpl();
    }

    /**
     * @param expectedVersion the version we currently expect the key to have. NEW if empty
     * @return the newly written version.
     */
    CompletableFuture<Long> put(String key, byte[] value, long expectedVersion);

    /**
     * @return the value, or null if it doesn't exist in the table.
     */
    CompletableFuture<Value> get(String key);

    /**
     * @param expectedVersion the version to delete, ANY to delete any version.
     */
    CompletableFuture<Void> delete(String key, long expectedVersion);

    CompletableFuture<Void> closeAsync();

    public interface Value {
        byte[] value();
        long version();
    }

    public class BadVersionException extends Exception {
    }
}
