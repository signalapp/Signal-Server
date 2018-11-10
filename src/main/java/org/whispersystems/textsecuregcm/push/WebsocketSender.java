/*
 * Copyright (C) 2014 Open WhisperSystems
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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import static org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;

public class WebsocketSender {

  public enum Type {
    APN,
    GCM,
    WEB
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(WebsocketSender.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final Meter websocketRequeueMeter = metricRegistry.meter(name(getClass(), "ws_requeue"));
  private final Meter websocketOnlineMeter  = metricRegistry.meter(name(getClass(), "ws_online"  ));
  private final Meter websocketOfflineMeter = metricRegistry.meter(name(getClass(), "ws_offline" ));

  private final Meter apnOnlineMeter        = metricRegistry.meter(name(getClass(), "apn_online" ));
  private final Meter apnOfflineMeter       = metricRegistry.meter(name(getClass(), "apn_offline"));

  private final Meter gcmOnlineMeter        = metricRegistry.meter(name(getClass(), "gcm_online" ));
  private final Meter gcmOfflineMeter       = metricRegistry.meter(name(getClass(), "gcm_offline"));

  private final Meter provisioningOnlineMeter  = metricRegistry.meter(name(getClass(), "provisioning_online" ));
  private final Meter provisioningOfflineMeter = metricRegistry.meter(name(getClass(), "provisioning_offline"));

  private final MessagesManager messagesManager;
  private final PubSubManager   pubSubManager;

  public WebsocketSender(MessagesManager messagesManager, PubSubManager pubSubManager) {
    this.messagesManager = messagesManager;
    this.pubSubManager   = pubSubManager;
  }

  public DeliveryStatus sendMessage(Account account, Device device, Envelope message, Type channel, boolean online) {
    WebsocketAddress address       = new WebsocketAddress(account.getNumber(), device.getId());
    PubSubMessage    pubSubMessage = PubSubMessage.newBuilder()
                                                  .setType(PubSubMessage.Type.DELIVER)
                                                  .setContent(message.toByteString())
                                                  .build();

    if (pubSubManager.publish(address, pubSubMessage)) {
      if      (channel == Type.APN) apnOnlineMeter.mark();
      else if (channel == Type.GCM) gcmOnlineMeter.mark();
      else                          websocketOnlineMeter.mark();

      return new DeliveryStatus(true);
    } else {
      if      (channel == Type.APN) apnOfflineMeter.mark();
      else if (channel == Type.GCM) gcmOfflineMeter.mark();
      else                          websocketOfflineMeter.mark();

      if (!online) queueMessage(account, device, message);
      return new DeliveryStatus(false);
    }
  }

  public void queueMessage(Account account, Device device, Envelope message) {
    websocketRequeueMeter.mark();

    WebsocketAddress address = new WebsocketAddress(account.getNumber(), device.getId());

    messagesManager.insert(account.getNumber(), device.getId(), message);
    pubSubManager.publish(address, PubSubMessage.newBuilder()
                                                .setType(PubSubMessage.Type.QUERY_DB)
                                                .build());
  }

  public boolean sendProvisioningMessage(ProvisioningAddress address, byte[] body) {
    PubSubMessage    pubSubMessage = PubSubMessage.newBuilder()
                                                  .setType(PubSubMessage.Type.DELIVER)
                                                  .setContent(ByteString.copyFrom(body))
                                                  .build();

    if (pubSubManager.publish(address, pubSubMessage)) {
      provisioningOnlineMeter.mark();
      return true;
    } else {
      provisioningOfflineMeter.mark();
      return false;
    }
  }

  static class DeliveryStatus {

    private final boolean delivered;

    DeliveryStatus(boolean delivered) {
      this.delivered = delivered;
    }

    boolean isDelivered() {
      return delivered;
    }

  }
}
