package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;

public class ApnFallbackManager implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ApnFallbackManager.class);

  private static final MetricRegistry metricRegistry          = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          voipOneSuccess          = metricRegistry.meter(name(ApnFallbackManager.class, "voip_one_success"));
  private static final Meter          voipOneDelivery         = metricRegistry.meter(name(ApnFallbackManager.class, "voip_one_failure"));
  private static final Histogram      voipOneSuccessHistogram = metricRegistry.histogram(name(ApnFallbackManager.class, "voip_one_success_histogram"));

  static {
    metricRegistry.register(name(ApnFallbackManager.class, "voip_one_success_ratio"), new VoipRatioGauge(voipOneSuccess, voipOneDelivery));
  }

  private final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();
  private final PushServiceClient pushServiceClient;

  public ApnFallbackManager(PushServiceClient pushServiceClient) {
    this.pushServiceClient = pushServiceClient;
  }

  public void schedule(final WebsocketAddress address, ApnFallbackTask task) {
    voipOneDelivery.mark();
    taskQueue.put(address, task);
  }

  public void cancel(WebsocketAddress address) {
    ApnFallbackTask task = taskQueue.remove(address);

    if (task != null) {
      voipOneSuccess.mark();
      voipOneSuccessHistogram.update(System.currentTimeMillis() - task.getScheduledTime());
    }
  }

  @Override
  public void start() throws Exception {
    new Thread(this).start();
  }

  @Override
  public void stop() throws Exception {

  }

  @Override
  public void run() {
    while (true) {
      try {
        Entry<WebsocketAddress, ApnFallbackTask> taskEntry  = taskQueue.get();
        ApnFallbackTask                          task       = taskEntry.getValue();

        pushServiceClient.send(new ApnMessage(task.getMessage(), task.getApnId(),
                                              false, ApnMessage.MAX_EXPIRATION));
      } catch (Throwable e) {
        logger.warn("ApnFallbackThread", e);
      }
    }
  }

  public static class ApnFallbackTask {

    private final long       delay;
    private final long       scheduledTime;
    private final String     apnId;
    private final ApnMessage message;

    public ApnFallbackTask(String apnId, ApnMessage message) {
      this(apnId, message, TimeUnit.SECONDS.toMillis(30));
    }

    @VisibleForTesting
    public ApnFallbackTask(String apnId, ApnMessage message, long delay) {
      this.scheduledTime = System.currentTimeMillis();
      this.delay         = delay;
      this.apnId         = apnId;
      this.message       = message;
    }

    public String getApnId() {
      return apnId;
    }

    public ApnMessage getMessage() {
      return message;
    }

    public long getScheduledTime() {
      return scheduledTime;
    }

    public long getExecutionTime() {
      return scheduledTime + delay;
    }

    public long getDelay() {
      return delay;
    }
  }

  @VisibleForTesting
  public static class ApnFallbackTaskQueue {

    private final LinkedHashMap<WebsocketAddress, ApnFallbackTask> tasks = new LinkedHashMap<>();

    public Entry<WebsocketAddress, ApnFallbackTask> get() {
      while (true) {
        long timeDelta;

        synchronized (tasks) {
          while (tasks.isEmpty()) Util.wait(tasks);

          Iterator<Entry<WebsocketAddress, ApnFallbackTask>> iterator  = tasks.entrySet().iterator();
          Entry<WebsocketAddress, ApnFallbackTask>           nextTask  = iterator.next();

          timeDelta = nextTask.getValue().getExecutionTime() - System.currentTimeMillis();

          if (timeDelta <= 0) {
            iterator.remove();
            return nextTask;
          }
        }

        Util.sleep(timeDelta);
      }
    }

    public void put(WebsocketAddress address, ApnFallbackTask task) {
      synchronized (tasks) {
        tasks.put(address, task);
        tasks.notifyAll();
      }
    }

    public ApnFallbackTask remove(WebsocketAddress address) {
      synchronized (tasks) {
        return tasks.remove(address);
      }
    }
  }

  private static class VoipRatioGauge extends RatioGauge {

    private final Meter success;
    private final Meter attempts;

    private VoipRatioGauge(Meter success, Meter attempts) {
      this.success  = success;
      this.attempts = attempts;
    }

    @Override
    protected Ratio getRatio() {
      return Ratio.of(success.getFiveMinuteRate(), attempts.getFiveMinuteRate());
    }
  }

}
