package io.pravega.connectors.flink;

import org.apache.flink.runtime.rescaling.OperatorRescalingPolicy;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.Observable;
import io.pravega.client.stream.notifications.SegmentNotification;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link OperatorRescalingPolicy} which scales to the number of current segments.
 */
@Slf4j
public class ReaderOperatorRescalingPolicy implements OperatorRescalingPolicy {

    private final ReaderGroup readerGroup;
    private final Observable<SegmentNotification> segmentNotifier;

    private volatile int currentNumberOfSegments;

    public ReaderOperatorRescalingPolicy(
            String readerGroupName,
            String scopeName,
            URI controllerURI,
            ScheduledExecutorService scheduledExecutorService) {

        this.readerGroup = ReaderGroupManager.withScope(scopeName, controllerURI).getReaderGroup(readerGroupName);
        this.currentNumberOfSegments = 1;

        this.segmentNotifier = readerGroup.getSegmentNotifier(scheduledExecutorService);
        segmentNotifier.registerListener(new ListenerImpl());
    }

    @Override
    public int rescaleTo(OperatorRescalingContext operatorRescalingContext) {
        return currentNumberOfSegments;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        segmentNotifier.unregisterAllListeners();
        return CompletableFuture.completedFuture(null);
    }

    private class ListenerImpl implements Listener<SegmentNotification> {

        @Override
        public void onNotification(SegmentNotification notification) {
            currentNumberOfSegments = notification.getNumOfSegments();
        }
    }
}
