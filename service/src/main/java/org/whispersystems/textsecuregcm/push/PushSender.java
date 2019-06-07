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
import org.whispersystems.textsecuregcm.push.WebsocketSender.DeliveryStatus;
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

  public PushSender(ApnFallbackManager apnFallbackManager,
                    GCMSender gcmSender, APNSender apnSender,
                    WebsocketSender websocketSender, int queueSize)
  {
    this.apnFallbackManager = apnFallbackManager;
    this.gcmSender          = gcmSender;
    this.apnSender          = apnSender;
    this.webSocketSender    = websocketSender;
    this.queueSize          = queueSize;
    this.executor           = new BlockingThreadPoolExecutor(50, queueSize);

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

  public void sendQueuedNotification(Account account, Device device)
      throws NotPushRegisteredException
  {
    if      (device.getGcmId() != null)    sendGcmNotification(account, device);
    else if (device.getApnId() != null)    sendApnNotification(account, device, true);
    else if (!device.getFetchesMessages()) throw new NotPushRegisteredException("No notification possible!");
  }

  public WebsocketSender getWebSocketSender() {
    return webSocketSender;
  }

  private void sendSynchronousMessage(Account account, Device device, Envelope message, boolean online) {
    if      (device.getGcmId() != null)   sendGcmMessage(account, device, message, online);
    else if (device.getApnId() != null)   sendApnMessage(account, device, message, online);
    else if (device.getFetchesMessages()) sendWebSocketMessage(account, device, message, online);
    else                                  throw new AssertionError();
  }

  private void sendGcmMessage(Account account, Device device, Envelope message, boolean online) {
    DeliveryStatus deliveryStatus = webSocketSender.sendMessage(account, device, message, WebsocketSender.Type.GCM, online);

    if (!deliveryStatus.isDelivered() && !online) {
      sendGcmNotification(account, device);
    }
  }

  private void sendGcmNotification(Account account, Device device) {
    GcmMessage gcmMessage = new GcmMessage(device.getGcmId(), account.getNumber(),
                                           (int)device.getId(), GcmMessage.Type.NOTIFICATION, Optional.empty());

    gcmSender.sendMessage(gcmMessage);
  }

  private void sendApnMessage(Account account, Device device, Envelope outgoingMessage, boolean online) {
    DeliveryStatus deliveryStatus = webSocketSender.sendMessage(account, device, outgoingMessage, WebsocketSender.Type.APN, online);

    if (!deliveryStatus.isDelivered() && outgoingMessage.getType() != Envelope.Type.RECEIPT && !online) {
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
