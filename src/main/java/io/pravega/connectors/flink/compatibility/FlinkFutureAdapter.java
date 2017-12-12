package io.pravega.connectors.flink.compatibility;

import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class FlinkFutureAdapter<T> extends CompletableFuture<T> implements Future<T> {

    public FlinkFutureAdapter(final CompletableFuture<T> baseFuture) {
        baseFuture.whenComplete(
            (T value, Throwable throwable) -> {
                if (value != null) {
                    this.complete(value);
                } else {
                    this.completeExceptionally(throwable);
                }
            });
    }

    @Override
    public <R> Future<R> thenApplyAsync(ApplyFunction<? super T, ? extends R> applyFunction, Executor executor) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenApplyAsync(applyFunction::apply, executor));
    }

    @Override
    public <R> Future<R> thenApply(ApplyFunction<? super T, ? extends R> applyFunction) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenApply(applyFunction::apply));
    }

    @Override
    public Future<Void> thenAcceptAsync(AcceptFunction<? super T> acceptFunction, Executor executor) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenAcceptAsync(acceptFunction::accept, executor));
    }

    @Override
    public Future<Void> thenAccept(AcceptFunction<? super T> acceptFunction) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenAccept(acceptFunction::accept));
    }

    @Override
    public <R> Future<R> exceptionallyAsync(ApplyFunction<Throwable, ? extends R> exceptionallyFunction, Executor executor) {
        final CompletableFuture<R> result = new CompletableFuture<>();

        this.whenCompleteAsync(
            (value, throwable) -> {
                if (value != null) {
                    try {
                        result.complete((R) value);
                    } catch (ClassCastException cce) {
                        // we have to do this due to the wrong type bound in Flink's Future interface definition
                        result.completeExceptionally(cce);
                    }
                } else {
                    result.completeExceptionally(throwable);
                }
            },
            executor);

        return new FlinkFutureAdapter<>(result);

    }

    @Override
    public <R> Future<R> exceptionally(ApplyFunction<Throwable, ? extends R> exceptionallyFunction) {
        final CompletableFuture<R> result = new CompletableFuture<>();

        this.whenComplete(
            (value, throwable) -> {
                if (value != null) {
                    try {
                        result.complete((R) value);
                    } catch (ClassCastException cce) {
                        // we have to do this due to the wrong type bound in Flink's Future interface definition
                        result.completeExceptionally(cce);
                    }
                } else {
                    result.completeExceptionally(throwable);
                }
            });

        return new FlinkFutureAdapter<>(result);
    }

    @Override
    public <R> Future<R> thenComposeAsync(ApplyFunction<? super T, ? extends Future<R>> composeFunction, Executor executor) {

        final CompletableFuture<R> result = new CompletableFuture<>();

        this.whenCompleteAsync(
            (value, throwable) -> {
                Future<R> apply = composeFunction.apply(value);

                apply.handleAsync(
                    (nestedValue, nestedThrowable) -> {
                        if (nestedThrowable != null) {
                            result.completeExceptionally(nestedThrowable);
                        } else {
                            result.complete(nestedValue);
                        }

                        return null;
                    },
                    executor);
            },
            executor);

        return new FlinkFutureAdapter<>(result);
    }

    @Override
    public <R> Future<R> thenCompose(ApplyFunction<? super T, ? extends Future<R>> composeFunction) {
        final CompletableFuture<R> result = new CompletableFuture<>();

        this.whenComplete(
                (value, throwable) -> {
                    Future<R> apply = composeFunction.apply(value);

                    apply.handle(
                            (nestedValue, nestedThrowable) -> {
                                if (nestedThrowable != null) {
                                    result.completeExceptionally(nestedThrowable);
                                } else {
                                    result.complete(nestedValue);
                                }

                                return null;
                            });
                });

        return new FlinkFutureAdapter<>(result);
    }

    @Override
    public <R> Future<R> handleAsync(BiFunction<? super T, Throwable, ? extends R> biFunction, Executor executor) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).handleAsync(
                biFunction::apply,
                executor));
    }

    @Override
    public <R> Future<R> handle(BiFunction<? super T, Throwable, ? extends R> biFunction) {
        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).handle(
                biFunction::apply));
    }

    @Override
    public <U, R> Future<R> thenCombineAsync(Future<U> other, BiFunction<? super T, ? super U, ? extends R> biFunction, Executor executor) {
        CompletableFuture<U> otherFuture = new CompletableFuture<>();

        other.handleAsync(
            (value, throwable) -> {
                if (throwable != null) {
                    otherFuture.completeExceptionally(throwable);
                } else {
                    otherFuture.complete(value);
                }

                return null;
            },
            executor);

        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenCombineAsync(
                otherFuture,
                biFunction::apply,
                executor));
    }

    @Override
    public <U, R> Future<R> thenCombine(Future<U> other, BiFunction<? super T, ? super U, ? extends R> biFunction) {
        CompletableFuture<U> otherFuture = new CompletableFuture<>();

        other.handle(
                (value, throwable) -> {
                    if (throwable != null) {
                        otherFuture.completeExceptionally(throwable);
                    } else {
                        otherFuture.complete(value);
                    }

                    return null;
                });

        return new FlinkFutureAdapter<>(((CompletableFuture<T>)this).thenCombine(
                otherFuture,
                biFunction::apply));
    }
}
