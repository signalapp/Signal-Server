package org.whispersystems.textsecuregcm.metrics;

import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class MicrometerLettuceCommandLatencyRecorder implements CommandLatencyRecorder {

    private static final String FIRST_RESPONSE_TIMER_NAME = name(MicrometerLettuceCommandLatencyRecorder.class, "firstResponse");
    private static final String COMPLETION_TIMER_NAME     = name(MicrometerLettuceCommandLatencyRecorder.class, "completion");

    @Override
    public void recordCommandLatency(final SocketAddress local, final SocketAddress remote, final ProtocolKeyword commandType, final long firstResponseLatency, final long completionLatency) {
        final List<Tag> tags = List.of(Tag.of("redisHost", remote.toString()), Tag.of("command", commandType.name()));

        Metrics.timer(FIRST_RESPONSE_TIMER_NAME, tags).record(firstResponseLatency, TimeUnit.NANOSECONDS);
        Metrics.timer(COMPLETION_TIMER_NAME, tags).record(completionLatency, TimeUnit.NANOSECONDS);
    }
}
