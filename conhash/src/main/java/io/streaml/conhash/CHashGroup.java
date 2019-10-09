package io.streaml.conhash;

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


import java.util.List;

public interface CHashGroup {
    /**
     * get the advertised identity of the current node
     */
    String localIdentity();

    State currentState();

    /**
     * Listener is triggered when the hash ring changes.
     */
    void registerListener(Runnable r);
    void unregisterListener(Runnable r);

    interface State {
        /**
         * get the advertised identity of the node who owns the passed in resource
         */
        String lookupOwner(String resource);

        /**
         * Get a sorted list of all owners.
         */
        List<String> allOwners();
    }
}
