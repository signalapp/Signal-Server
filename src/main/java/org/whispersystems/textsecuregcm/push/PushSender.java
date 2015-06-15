/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ApnMessage;
import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.GcmMessage;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTask;
import org.whispersystems.textsecuregcm.push.WebsocketSender.DeliveryStatus;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.concurrent.TimeUnit;

import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

public class PushSender {

  private final Logger logger = LoggerFactory.getLogger(PushSender.class);

  private static final String APN_PAYLOAD = "{\"aps\":{\"sound\":\"default\",\"badge\":%d,\"alert\":{\"loc-key\":\"APN_Message\"}}}";

  private final ApnFallbackManager apnFallbackManager;
  private final PushServiceClient  pushServiceClient;
  private final WebsocketSender    webSocketSender;

  public PushSender(ApnFallbackManager apnFallbackManager, PushServiceClient pushServiceClient, WebsocketSender websocketSender) {
    this.apnFallbackManager = apnFallbackManager;
    this.pushServiceClient  = pushServiceClient;
    this.webSocketSender    = websocketSender;
  }

  public void sendMessage(Account account, Device device, Envelope message)
      throws NotPushRegisteredException, TransientPushFailureException
  {
    if      (device.getGcmId() != null)   sendGcmMessage(account, device, message);
    else if (device.getApnId() != null)   sendApnMessage(account, device, message);
    else if (device.getFetchesMessages()) sendWebSocketMessage(account, device, message);
    else                                  throw new NotPushRegisteredException("No delivery possible!");
  }

  public WebsocketSender getWebSocketSender() {
    return webSocketSender;
  }

  private void sendGcmMessage(Account account, Device device, Envelope message)
      throws TransientPushFailureException, NotPushRegisteredException
  {
    if (device.getFetchesMessages()) sendNotificationGcmMessage(account, device, message);
    else                             sendPayloadGcmMessage(account, device, message);
  }

  private void sendPayloadGcmMessage(Account account, Device device, Envelope message)
      throws TransientPushFailureException, NotPushRegisteredException
  {
    try {
      String                   number           = account.getNumber();
      long                     deviceId         = device.getId();
      String                   registrationId   = device.getGcmId();
      boolean                  isReceipt        = message.getType() == Envelope.Type.RECEIPT;
      EncryptedOutgoingMessage encryptedMessage = new EncryptedOutgoingMessage(message, device.getSignalingKey());
      GcmMessage               gcmMessage       = new GcmMessage(registrationId, number, (int) deviceId,
                                                                 encryptedMessage.toEncodedString(), isReceipt, false);

      pushServiceClient.send(gcmMessage);
    } catch (CryptoEncodingException e) {
      throw new NotPushRegisteredException(e);
    }
  }

  private void sendNotificationGcmMessage(Account account, Device device, Envelope message)
      throws TransientPushFailureException
  {
    DeliveryStatus deliveryStatus = webSocketSender.sendMessage(account, device, message, WebsocketSender.Type.GCM);

    if (!deliveryStatus.isDelivered()) {
      GcmMessage gcmMessage = new GcmMessage(device.getGcmId(), account.getNumber(),
                                             (int)device.getId(), "", false, true);

      pushServiceClient.send(gcmMessage);
    }
  }

  private void sendApnMessage(Account account, Device device, Envelope outgoingMessage)
      throws TransientPushFailureException
  {
    DeliveryStatus deliveryStatus = webSocketSender.sendMessage(account, device, outgoingMessage, WebsocketSender.Type.APN);

    if (!deliveryStatus.isDelivered() && outgoingMessage.getType() != Envelope.Type.RECEIPT) {
      ApnMessage apnMessage;

      if (!Util.isEmpty(device.getVoipApnId())) {
        apnMessage = new ApnMessage(device.getVoipApnId(), account.getNumber(), (int)device.getId(),
                                    String.format(APN_PAYLOAD, deliveryStatus.getMessageQueueDepth()),
                                    true, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30));

        apnFallbackManager.schedule(new WebsocketAddress(account.getNumber(), device.getId()),
                                    new ApnFallbackTask(device.getApnId(), apnMessage));
      } else {
        apnMessage = new ApnMessage(device.getApnId(), account.getNumber(), (int)device.getId(),
                                    String.format(APN_PAYLOAD, deliveryStatus.getMessageQueueDepth()),
                                    false, ApnMessage.MAX_EXPIRATION);
      }

      pushServiceClient.send(apnMessage);
    }
  }

  private void sendWebSocketMessage(Account account, Device device, Envelope outgoingMessage)
  {
    webSocketSender.sendMessage(account, device, outgoingMessage, WebsocketSender.Type.WEB);
  }
}
