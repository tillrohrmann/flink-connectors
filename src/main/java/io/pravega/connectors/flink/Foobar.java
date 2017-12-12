package io.pravega.connectors.flink;

import org.apache.flink.runtime.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface Foobar<T> {

    Future<T> triggerCheckpoint(long checkpointId, long timestamp, Executor executor) throws Exception;
}
