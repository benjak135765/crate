/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect;

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.ListenableRowConsumer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.CompletionState;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.metadata.RowGranularity;
import io.netty.util.collection.IntObjectHashMap;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class CollectTask implements Task {

    private final CollectPhase collectPhase;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final ListenableRowConsumer consumer;
    private final SharedShardContexts sharedShardContexts;

    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final String threadPoolName;

    private final AtomicReference<State> currentState = new AtomicReference<>(State.CREATED);
    private final CompletableFuture<CompletionState> completionFuture;

    private BatchIterator<Row> batchIterator = null;

    public CollectTask(final CollectPhase collectPhase,
                       MapSideDataCollectOperation collectOperation,
                       RamAccountingContext queryPhaseRamAccountingContext,
                       RowConsumer consumer,
                       SharedShardContexts sharedShardContexts) {
        this.collectPhase = collectPhase;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.sharedShardContexts = sharedShardContexts;
        this.consumer = new ListenableRowConsumer(consumer);
        this.completionFuture = this.consumer.completionFuture().handle((result, ex) -> {
            searchers.values().forEach(Engine.Searcher::close);
            searchers.clear();
            CompletionState completionState = new CompletionState();
            completionState.bytesUsed(queryPhaseRamAccountingContext.totalBytes());
            queryPhaseRamAccountingContext.close();
            currentState.set(State.STOPPED);
            if (ex == null) {
                return completionState;
            } else if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        });
        this.threadPoolName = threadPoolName(collectPhase);
    }

    void addSearcher(int searcherId, Engine.Searcher searcher) {
        synchronized (searchers) {
            State state = currentState.get();
            if (state != State.CREATED) {
                searcher.close();
                return;
            }
            Engine.Searcher replacedSearcher = searchers.put(searcherId, searcher);
            if (replacedSearcher != null) {
                searchers.values().forEach(Engine.Searcher::close);
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "ShardCollectContext for %d already added", searcherId));
            }
        }
    }

    @Override
    public String name() {
        return collectPhase.name();
    }

    @Override
    public int id() {
        return collectPhase.phaseId();
    }


    @Override
    public String toString() {
        return "CollectTask{" +
               "id=" + collectPhase.phaseId() +
               ", sharedContexts=" + sharedShardContexts +
               ", consumer=" + consumer +
               ", searchContexts=" + searchers.keySet() +
               ", state=" + currentState.get() +
               '}';
    }

    public void start() {
        try {
            batchIterator = collectOperation.createIterator(collectPhase, consumer.requiresScroll(), this);
        } catch (Throwable t) {
            kill(t);
            return;
        }
        if (currentState.compareAndSet(State.CREATED, State.RUNNING)) {
            try {
                collectOperation.launch(() -> consumer.accept(batchIterator, null), threadPoolName);
            } catch (Throwable t) {
                consumer.accept(null, t);
                throw t;
            }
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        State prevState;
        synchronized (searchers) {
            prevState = currentState.getAndSet(State.STOPPED);
            searchers.values().forEach(Engine.Searcher::close);
        }
        switch (prevState) {
            case CREATED:
                try {
                    consumer.accept(null, throwable);
                } catch (Throwable t) {
                    throw t;
                }
                return;

            case RUNNING:
                batchIterator.kill(throwable);
                return;

            case STOPPED:
                // Nothing to do
                break;

            default:
                throw new AssertionError("Invalid state: " + prevState);
        }
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    public SharedShardContexts sharedShardContexts() {
        return sharedShardContexts;
    }

    @VisibleForTesting
    static String threadPoolName(CollectPhase phase) {
        if (phase instanceof RoutedCollectPhase) {
            RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
            if (collectPhase.maxRowGranularity() == RowGranularity.NODE
                       || collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
                // Node or Shard system table collector
                return ThreadPool.Names.GET;
            }
        }

        // Anything else like doc tables, INFORMATION_SCHEMA tables or sys.cluster table collector, partition collector
        return ThreadPool.Names.SEARCH;
    }

    @Override
    public CompletableFuture<CompletionState> completionFuture() {
        return completionFuture;
    }
}
