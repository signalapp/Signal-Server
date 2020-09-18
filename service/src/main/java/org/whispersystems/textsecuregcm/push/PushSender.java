/*
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.BlockingThreadPoolExecutor;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

public class PushSender implements Managed {

  @SuppressWarnings("unused")
  private final Logger logger = LoggerFactory.getLogger(PushSender.class);

  private final ApnFallbackManager         apnFallbackManager;
  private final GCMSender                  gcmSender;
  private final APNSender                  apnSender;
  private final WebsocketSender            webSocketSender;
  private final BlockingThreadPoolExecutor executor;
  private final int                        queueSize;
  private final PushLatencyManager         pushLatencyManager;

  public PushSender(ApnFallbackManager apnFallbackManager,
                    GCMSender gcmSender, APNSender apnSender,
                    WebsocketSender websocketSender, int queueSize,
                    PushLatencyManager pushLatencyManager)
  {
    this.apnFallbackManager = apnFallbackManager;
    this.gcmSender          = gcmSender;
    this.apnSender          = apnSender;
    this.webSocketSender    = websocketSender;
    this.queueSize          = queueSize;
    this.executor           = new BlockingThreadPoolExecutor("pushSender", 50, queueSize);
    this.pushLatencyManager = pushLatencyManager;

    SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME)
                          .register(name(PushSender.class, "send_queue_depth"),
                                    (Gauge<Integer>) executor::getSize);
  }

  public void sendMessage(final Account account, final Device device, final Envelope message, boolean online)
      throws NotPushRegisteredException
  {
    if (device.getGcmId() == null && device.getApnId() == null && !device.getFetchesMessages()) {
      throw new NotPushRegisteredException("No delivery possible!");
    }

    if (queueSize > 0) {
      executor.execute(() -> sendSynchronousMessage(account, device, message, online));
    } else {
      sendSynchronousMessage(account, device, message, online);
    }
  }

  private void sendSynchronousMessage(Account account, Device device, Envelope message, boolean online) {
    if      (device.getGcmId() != null)   sendGcmMessage(account, device, message, online);
    else if (device.getApnId() != null)   sendApnMessage(account, device, message, online);
    else if (device.getFetchesMessages()) sendWebSocketMessage(account, device, message, online);
    else                                  throw new AssertionError();
  }

  private void sendGcmMessage(Account account, Device device, Envelope message, boolean online) {
    final boolean delivered = webSocketSender.sendMessage(account, device, message, WebsocketSender.Type.GCM, online);

    if (!delivered && !online) {
      sendGcmNotification(account, device);
    }
  }

  private void sendGcmNotification(Account account, Device device) {
    GcmMessage gcmMessage = new GcmMessage(device.getGcmId(), account.getNumber(),
                                           (int)device.getId(), GcmMessage.Type.NOTIFICATION, Optional.empty());

    gcmSender.sendMessage(gcmMessage);

    RedisOperation.unchecked(() -> pushLatencyManager.recordPushSent(account.getUuid(), device.getId()));
  }

  private void sendApnMessage(Account account, Device device, Envelope outgoingMessage, boolean online) {
    final boolean delivered = webSocketSender.sendMessage(account, device, outgoingMessage, WebsocketSender.Type.APN, online);

    if (!delivered && outgoingMessage.getType() != Envelope.Type.RECEIPT && !online) {
      sendApnNotification(account, device, false);
    }
  }

  private void sendApnNotification(Account account, Device device, boolean newOnly) {
    ApnMessage apnMessage;

    if (newOnly && RedisOperation.unchecked(() -> apnFallbackManager.isScheduled(account, device))) {
      return;
    }

    if (!Util.isEmpty(device.getVoipApnId())) {
      apnMessage = new ApnMessage(device.getVoipApnId(), account.getNumber(), device.getId(), true, Optional.empty());
      RedisOperation.unchecked(() -> apnFallbackManager.schedule(account, device));
    } else {
      apnMessage = new ApnMessage(device.getApnId(), account.getNumber(), device.getId(), false, Optional.empty());
    }

    apnSender.sendMessage(apnMessage);

    RedisOperation.unchecked(() -> pushLatencyManager.recordPushSent(account.getUuid(), device.getId()));
  }

  private void sendWebSocketMessage(Account account, Device device, Envelope outgoingMessage, boolean online)
  {
    webSocketSender.sendMessage(account, device, outgoingMessage, WebsocketSender.Type.WEB, online);
  }

  @Override
  public void start() throws Exception {
    apnSender.start();
    gcmSender.start();
  }

  @Override
  public void stop() throws Exception {
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.MINUTES);

    apnSender.stop();
    gcmSender.stop();
  }

}
