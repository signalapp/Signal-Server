package org.whispersystems.textsecuregcm.experiment;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyVararg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExperimentTest {

    private MeterRegistry meterRegistry;

    @Before
    public void setUp() {
        meterRegistry = mock(MeterRegistry.class);
    }

    @Test
    public void compareResultMatch() {
        final Timer timer = mock(Timer.class);
        when(meterRegistry.timer(anyString(), ArgumentMatchers.<Iterable<Tag>>any())).thenReturn(timer);

        new Experiment(meterRegistry, "test").compareResult(12, CompletableFuture.completedFuture(12));

        @SuppressWarnings("unchecked") final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);

        verify(meterRegistry).timer(anyString(), tagCaptor.capture());

        final Set<Tag> tags = getTagSet(tagCaptor.getValue());
        assertEquals(tags, Set.of(Tag.of(Experiment.OUTCOME_TAG, Experiment.MATCH_OUTCOME)));

        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareResultMismatch() {
        final Timer timer = mock(Timer.class);
        when(meterRegistry.timer(anyString(), ArgumentMatchers.<Iterable<Tag>>any())).thenReturn(timer);

        new Experiment(meterRegistry, "test").compareResult(12, CompletableFuture.completedFuture(77));

        @SuppressWarnings("unchecked") final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);

        verify(meterRegistry).timer(anyString(), tagCaptor.capture());

        final Set<Tag> tags = getTagSet(tagCaptor.getValue());
        assertEquals(tags, Set.of(Tag.of(Experiment.OUTCOME_TAG, Experiment.MISMATCH_OUTCOME)));

        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareResultError() {
        final Timer timer = mock(Timer.class);
        when(meterRegistry.timer(anyString(), ArgumentMatchers.<Iterable<Tag>>any())).thenReturn(timer);

        new Experiment(meterRegistry, "test").compareResult(12, CompletableFuture.failedFuture(new RuntimeException("OH NO")));

        @SuppressWarnings("unchecked") final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);

        verify(meterRegistry).timer(anyString(), tagCaptor.capture());

        final Set<Tag> tags = getTagSet(tagCaptor.getValue());
        assertEquals(tags, Set.of(Tag.of(Experiment.OUTCOME_TAG, Experiment.ERROR_OUTCOME), Tag.of(Experiment.CAUSE_TAG, "RuntimeException")));

        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    public void compareResultNoExperimentData() {
        final Timer timer = mock(Timer.class);
        when(meterRegistry.timer(anyString(), ArgumentMatchers.<Iterable<Tag>>any())).thenReturn(timer);

        new Experiment(meterRegistry, "test").compareResult(12, CompletableFuture.completedFuture(null));

        verify(timer, never()).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    private static Set<Tag> getTagSet(final Iterable<Tag> tags) {
        final Set<Tag> tagSet = new HashSet<>();

        for (final Tag tag : tags) {
            tagSet.add(tag);
        }

        return tagSet;
    }
}
