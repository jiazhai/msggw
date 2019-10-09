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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.Lists;
import io.streaml.conhash.CHashGroup;

class MockCHashGroup implements CHashGroup {
    private final String advertizedAddress;
    private final AtomicReference<CHashGroup.State> currentState = new AtomicReference<>();
    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

    MockCHashGroup(String advertizedAddress) {
        this.advertizedAddress = advertizedAddress;

        List<String> owners = Lists.newArrayList(advertizedAddress);
        currentState.set(new CHashGroup.State() {
                @Override
                public String lookupOwner(String resource) {
                    return advertizedAddress;
                }
                @Override
                public List<String> allOwners() {
                    return owners;
                }
            });
    }

    @Override
    public String localIdentity() {
        return advertizedAddress;
    }

    @Override
    public void registerListener(Runnable r) {
        listeners.add(r);
    }

    @Override
    public void unregisterListener(Runnable r) {
        listeners.remove(r);
    }

    @Override
    public CHashGroup.State currentState() {
        return currentState.get();
    }

    void addAssignmentForResource(String overriddenAddress, String overriddenResource) {
        CHashGroup.State previousState = currentState.get();
        List<String> owners = Lists.newArrayList(previousState.allOwners());
        owners.add(overriddenAddress);
        currentState.set(new CHashGroup.State() {
                @Override
                public String lookupOwner(String resource) {
                    if (resource.equals(overriddenResource)) {
                        return overriddenAddress;
                    } else {
                        return previousState.lookupOwner(resource);
                    }
                }

                @Override
                public List<String> allOwners() {
                    return owners;
                }
            });
        for (Runnable r : listeners) {
            r.run();
        }
    }
}
