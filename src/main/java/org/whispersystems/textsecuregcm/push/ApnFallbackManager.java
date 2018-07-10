package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.DispatchChannel;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnectionInfo;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;

public class ApnFallbackManager implements Managed, Runnable, DispatchChannel {

  private static final Logger logger = LoggerFactory.getLogger(ApnFallbackManager.class);

  public static final int FALLBACK_DURATION = 15;

  private static final MetricRegistry metricRegistry          = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          voipOneSuccess          = metricRegistry.meter(name(ApnFallbackManager.class, "voip_one_success"));
  private static final Meter          voipOneDelivery         = metricRegistry.meter(name(ApnFallbackManager.class, "voip_one_failure"));
  private static final Histogram      voipOneSuccessHistogram = metricRegistry.histogram(name(ApnFallbackManager.class, "voip_one_success_histogram"));

  static {
    metricRegistry.register(name(ApnFallbackManager.class, "voip_one_success_ratio"), new VoipRatioGauge(voipOneSuccess, voipOneDelivery));
  }

  private final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();

  private final APNSender     apnSender;
  private final PubSubManager pubSubManager;

  public ApnFallbackManager(APNSender apnSender, PubSubManager pubSubManager) {
    this.apnSender     = apnSender;
    this.pubSubManager = pubSubManager;
  }

  public void schedule(final WebsocketAddress address, ApnFallbackTask task) {
    voipOneDelivery.mark();

    if (taskQueue.put(address, task)) {
      pubSubManager.subscribe(new WebSocketConnectionInfo(address), this);
    }
  }

  private void scheduleRetry(final WebsocketAddress address, ApnFallbackTask task) {
    if (taskQueue.putIfMissing(address, task)) {
      pubSubManager.subscribe(new WebSocketConnectionInfo(address), this);
    }
  }

  public void cancel(WebsocketAddress address) {
    ApnFallbackTask task = taskQueue.remove(address);

    if (task != null) {
      pubSubManager.unsubscribe(new WebSocketConnectionInfo(address), this);
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

        ApnMessage message;

        if (task.getAttempt() == 0) {
          message = new ApnMessage(task.getMessage(), task.getVoipApnId(), true, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(FALLBACK_DURATION));
          scheduleRetry(taskEntry.getKey(), new ApnFallbackTask(task.getApnId(), task.getVoipApnId(), task.getMessage(), task.getDelay(),1));
        } else {
          message = new ApnMessage(task.getMessage(), task.getApnId(), false, ApnMessage.MAX_EXPIRATION);
          pubSubManager.unsubscribe(new WebSocketConnectionInfo(taskEntry.getKey()), this);
        }

        apnSender.sendMessage(message);
      } catch (Throwable e) {
        logger.warn("ApnFallbackThread", e);
      }
    }
  }

  @Override
  public void onDispatchMessage(String channel, byte[] message) {
    try {
      PubSubMessage notification = PubSubMessage.parseFrom(message);

      if (notification.getType().getNumber() == PubSubMessage.Type.CONNECTED_VALUE) {
        WebSocketConnectionInfo address = new WebSocketConnectionInfo(channel);
        cancel(address.getWebsocketAddress());
      } else {
        logger.warn("Got strange pubsub type: " + notification.getType().getNumber());
      }

    } catch (WebSocketConnectionInfo.FormattingException e) {
      logger.warn("Bad formatting?", e);
    } catch (InvalidProtocolBufferException e) {
      logger.warn("Bad protobuf", e);
    }
  }

  @Override
  public void onDispatchSubscribed(String channel) {}

  @Override
  public void onDispatchUnsubscribed(String channel) {}

  public static class ApnFallbackTask {

    private final long       delay;
    private final long       scheduledTime;
    private final String     apnId;
    private final String     voipApnId;
    private final ApnMessage message;
    private final int        attempt;

    public ApnFallbackTask(String apnId, String voipApnId, ApnMessage message) {
      this(apnId, voipApnId, message, TimeUnit.SECONDS.toMillis(FALLBACK_DURATION), 0);
    }

    @VisibleForTesting
    public ApnFallbackTask(String apnId, String voipApnId, ApnMessage message, long delay, int attempt) {
      this.scheduledTime = System.currentTimeMillis();
      this.delay         = delay;
      this.apnId         = apnId;
      this.voipApnId     = voipApnId;
      this.message       = message;
      this.attempt       = attempt;
    }

    public String getApnId() {
      return apnId;
    }

    public String getVoipApnId() {
      return voipApnId;
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

    public int getAttempt() {
      return attempt;
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

    public boolean put(WebsocketAddress address, ApnFallbackTask task) {
      synchronized (tasks) {
        ApnFallbackTask previous = tasks.put(address, task);
        tasks.notifyAll();

        return previous == null;
      }
    }

    public boolean putIfMissing(WebsocketAddress address, ApnFallbackTask task) {
      synchronized (tasks) {
        if (tasks.containsKey(address)) return false;
        return put(address, task);
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
