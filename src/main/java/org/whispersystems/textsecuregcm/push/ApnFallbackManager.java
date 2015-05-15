package org.whispersystems.textsecuregcm.push;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import io.dropwizard.lifecycle.Managed;

public class ApnFallbackManager implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ApnFallbackManager.class);

  private final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();
  private final PushServiceClient pushServiceClient;

  public ApnFallbackManager(PushServiceClient pushServiceClient) {
    this.pushServiceClient = pushServiceClient;
  }

  public void schedule(final WebsocketAddress address, ApnFallbackTask task) {
    taskQueue.put(address, task);
  }

  public void cancel(WebsocketAddress address) {
    taskQueue.remove(address);
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
        int                                      retryCount = task.getRetryCount();

        if (retryCount == 0) {
          pushServiceClient.send(task.getMessage());
          schedule(taskEntry.getKey(), new ApnFallbackTask(task.getApnId(), task.getMessage(),
                                                           retryCount + 1, task.getDelay()));
        } else if (retryCount == 1) {
          pushServiceClient.send(new ApnMessage(task.getMessage(), task.getApnId(), false));
        }
      } catch (Throwable e) {
        logger.warn("ApnFallbackThread", e);
      }
    }
  }

  public static class ApnFallbackTask {

    private final long       delay;
    private final long       executionTime;
    private final String     apnId;
    private final ApnMessage message;
    private final int        retryCount;

    public ApnFallbackTask(String apnId, ApnMessage message, int retryCount) {
      this(apnId, message, retryCount, TimeUnit.SECONDS.toMillis(15));
    }

    @VisibleForTesting
    public ApnFallbackTask(String apnId, ApnMessage message, int retryCount, long delay) {
      this.executionTime = System.currentTimeMillis() + delay;
      this.delay         = delay;
      this.apnId         = apnId;
      this.message       = message;
      this.retryCount    = retryCount;
    }

    public String getApnId() {
      return apnId;
    }

    public ApnMessage getMessage() {
      return message;
    }

    public int getRetryCount() {
      return retryCount;
    }

    public long getExecutionTime() {
      return executionTime;
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

    public void remove(WebsocketAddress address) {
      synchronized (tasks) {
        tasks.remove(address);
      }
    }
  }

}
