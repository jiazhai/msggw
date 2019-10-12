package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

public class ManagedLedgerInitializeLedgerCallbackPromise
    extends CompletableFuture<Void>
    implements ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback {
    @Override
    public void initializeComplete() {
        complete(null);
    }

    @Override
    public void initializeFailed(ManagedLedgerException e) {
        completeExceptionally(e);
    }

    public static Class<?> parentClass() {
        return ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback.class;
    }
}
