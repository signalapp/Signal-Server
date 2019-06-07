package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.RedisException;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class ApnFallbackManager implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ApnFallbackManager.class);

  private static final String PENDING_NOTIFICATIONS_KEY = "PENDING_APN";

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          delivered      = metricRegistry.meter(name(ApnFallbackManager.class, "voip_delivered"));
  private static final Meter          sent           = metricRegistry.meter(name(ApnFallbackManager.class, "voip_sent"     ));
  private static final Meter          retry          = metricRegistry.meter(name(ApnFallbackManager.class, "voip_retry"));
  private static final Meter          evicted        = metricRegistry.meter(name(ApnFallbackManager.class, "voip_evicted"));

  static {
    metricRegistry.register(name(ApnFallbackManager.class, "voip_ratio"), new VoipRatioGauge(delivered, sent));
  }

  private final APNSender       apnSender;
  private final AccountsManager accountsManager;

  private final ReplicatedJedisPool jedisPool;
  private final InsertOperation     insertOperation;
  private final GetOperation        getOperation;
  private final RemoveOperation     removeOperation;


  private AtomicBoolean running = new AtomicBoolean(false);
  private boolean finished;

  public ApnFallbackManager(ReplicatedJedisPool jedisPool,
                            APNSender apnSender,
                            AccountsManager accountsManager)
      throws IOException
  {
    this.apnSender       = apnSender;
    this.accountsManager = accountsManager;
    this.jedisPool       = jedisPool;
    this.insertOperation = new InsertOperation(jedisPool);
    this.getOperation    = new GetOperation(jedisPool);
    this.removeOperation = new RemoveOperation(jedisPool);
  }

  public void schedule(Account account, Device device) throws RedisException {
    try {
      sent.mark();
      insertOperation.insert(account, device, System.currentTimeMillis() + (15 * 1000), (15 * 1000));
    } catch (JedisException e) {
      throw new RedisException(e);
    }
  }

  public boolean isScheduled(Account account, Device device) throws RedisException {
    try {
      String endpoint = "apn_device::" + account.getNumber() + "::" + device.getId();

      try (Jedis jedis = jedisPool.getReadResource()) {
        return jedis.zscore(PENDING_NOTIFICATIONS_KEY, endpoint) != null;
      }
    } catch (JedisException e) {
      throw new RedisException(e);
    }
  }

  public void cancel(Account account, Device device) throws RedisException {
    try {
      if (removeOperation.remove(account, device)) {
        delivered.mark();
      }
    } catch (JedisException e) {
      throw new RedisException(e);
    }
  }

  @Override
  public synchronized void start() {
    running.set(true);
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() {
    running.set(false);
    while (!finished) Util.wait(this);
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        List<byte[]> pendingNotifications = getOperation.getPending(100);

        for (byte[] pendingNotification : pendingNotifications) {
          String                       numberAndDevice = new String(pendingNotification);
          Optional<Pair<String, Long>> separated       = getSeparated(numberAndDevice);

          if (!separated.isPresent()) {
            removeOperation.remove(numberAndDevice);
            continue;
          }

          Optional<Account> account = accountsManager.get(separated.get().first());

          if (!account.isPresent()) {
            removeOperation.remove(numberAndDevice);
            continue;
          }

          Optional<Device> device = account.get().getDevice(separated.get().second());

          if (!device.isPresent()) {
            removeOperation.remove(numberAndDevice);
            continue;
          }

          String apnId = device.get().getVoipApnId();

          if (apnId == null) {
            removeOperation.remove(account.get(), device.get());
            continue;
          }

          long deviceLastSeen = device.get().getLastSeen();

          if (deviceLastSeen < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(90)) {
            evicted.mark();
            removeOperation.remove(account.get(), device.get());
            continue;
          }

          apnSender.sendMessage(new ApnMessage(apnId, separated.get().first(), separated.get().second(), true, Optional.empty()));
          retry.mark();
        }

      } catch (Exception e) {
        logger.warn("Exception while operating", e);
      }

      Util.sleep(1000);
    }

    synchronized (ApnFallbackManager.this) {
      finished = true;
      notifyAll();
    }
  }

  private Optional<Pair<String, Long>> getSeparated(String encoded) {
    try {
      if (encoded == null) return Optional.empty();

      String[] parts = encoded.split(":");

      if (parts.length != 2) {
        logger.warn("Got strange encoded number: " + encoded);
        return Optional.empty();
      }

      return Optional.of(new Pair<>(parts[0], Long.parseLong(parts[1])));
    } catch (NumberFormatException e) {
      logger.warn("Badly formatted: " + encoded, e);
      return Optional.empty();
    }
  }

  private static class RemoveOperation {

    private final LuaScript luaScript;

    RemoveOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.luaScript = LuaScript.fromResource(jedisPool, "lua/apn/remove.lua");
    }

    boolean remove(Account account, Device device) {
      String endpoint = "apn_device::" + account.getNumber() + "::" + device.getId();
      return remove(endpoint);
    }

    boolean remove(String endpoint) {
      if (!PENDING_NOTIFICATIONS_KEY.equals(endpoint)) {
        List<byte[]> keys = Arrays.asList(PENDING_NOTIFICATIONS_KEY.getBytes(), endpoint.getBytes());
        List<byte[]> args = Collections.emptyList();

        return ((long)luaScript.execute(keys, args)) > 0;
      }

      return false;
    }

  }

  private static class GetOperation {

    private final LuaScript luaScript;

    GetOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.luaScript = LuaScript.fromResource(jedisPool, "lua/apn/get.lua");
    }

    @SuppressWarnings("SameParameterValue")
    List<byte[]> getPending(int limit) {
      List<byte[]> keys = Arrays.asList(PENDING_NOTIFICATIONS_KEY.getBytes());
      List<byte[]> args = Arrays.asList(String.valueOf(System.currentTimeMillis()).getBytes(), String.valueOf(limit).getBytes());

      return (List<byte[]>) luaScript.execute(keys, args);
    }
  }

  private static class InsertOperation {

    private final LuaScript luaScript;

    InsertOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.luaScript = LuaScript.fromResource(jedisPool, "lua/apn/insert.lua");
    }

    public void insert(Account account, Device device, long timestamp, long interval) {
      String endpoint = "apn_device::" + account.getNumber() + "::" + device.getId();

      List<byte[]> keys = Arrays.asList(PENDING_NOTIFICATIONS_KEY.getBytes(), endpoint.getBytes());
      List<byte[]> args = Arrays.asList(String.valueOf(timestamp).getBytes(), String.valueOf(interval).getBytes(),
                                        account.getNumber().getBytes(), String.valueOf(device.getId()).getBytes());

      luaScript.execute(keys, args);
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
      return RatioGauge.Ratio.of(success.getFiveMinuteRate(), attempts.getFiveMinuteRate());
    }
  }

}
