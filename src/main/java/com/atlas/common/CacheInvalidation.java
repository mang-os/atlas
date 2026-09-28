package com.atlas.common;

import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class CacheInvalidation {
    private final RedisCacheStore store;
    public CacheInvalidation(ObjectProvider<RedisCacheStore> store) { this.store=store.getIfAvailable(); }
    public void afterCommit(UUID resource, UUID reservation) {
        if (store==null) return;
        if (!TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("Cache invalidation requires a business transaction");
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try {
                    if (reservation==null) store.invalidate(ReadCache.availabilityKey(resource));
                    else store.invalidate(ReadCache.availabilityKey(resource),ReadCache.reservationKey(reservation));
                } catch (RuntimeException failure) {
                    org.slf4j.LoggerFactory.getLogger(CacheInvalidation.class).warn("Cache invalidation failed after committed mutation category={}",failure.getClass().getSimpleName());
                }
            }
        });
    }
}
