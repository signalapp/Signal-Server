package org.whispersystems.textsecuregcm.util;

import io.micrometer.core.instrument.Timer;
import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class AsyncTimerUtil {
  @Nonnull
  public static <T> CompletionStage<T> record(final Timer timer, final Supplier<CompletionStage<T>> toRecord)  {
    final Timer.Sample sample = Timer.start();
    return toRecord.get().whenComplete((ignoreT, ignoreE) -> sample.stop(timer));
  }

}
