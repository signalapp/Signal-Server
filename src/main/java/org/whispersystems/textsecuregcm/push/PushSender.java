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
import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.PendingMessage;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class PushSender {

  private final Logger logger = LoggerFactory.getLogger(PushSender.class);

  private final GCMSender       gcmSender;
  private final APNSender       apnSender;
  private final WebsocketSender webSocketSender;

  public PushSender(GCMSender gcmClient,
                    APNSender apnSender,
                    WebsocketSender websocketSender)
  {
    this.gcmSender       = gcmClient;
    this.apnSender       = apnSender;
    this.webSocketSender = websocketSender;
  }

  public void sendMessage(Account account, Device device, MessageProtos.OutgoingMessageSignal message)
      throws NotPushRegisteredException, TransientPushFailureException
  {
    try {
      String                   signalingKey     = device.getSignalingKey();
      EncryptedOutgoingMessage encryptedMessage = new EncryptedOutgoingMessage(message, signalingKey);
      PendingMessage           pendingMessage   = new PendingMessage(message.getSource(), message.getTimestamp(), encryptedMessage.serialize());

      sendMessage(account, device, pendingMessage);
    } catch (CryptoEncodingException e) {
      throw new NotPushRegisteredException(e);
    }
  }

  public void sendMessage(Account account, Device device, PendingMessage pendingMessage)
      throws NotPushRegisteredException, TransientPushFailureException
  {
    if      (device.getGcmId() != null)   sendGcmMessage(account, device, pendingMessage);
    else if (device.getApnId() != null)   sendApnMessage(account, device, pendingMessage);
    else if (device.getFetchesMessages()) sendWebSocketMessage(account, device, pendingMessage);
    else                                  throw new NotPushRegisteredException("No delivery possible!");
  }

  private void sendGcmMessage(Account account, Device device, PendingMessage pendingMessage) {
    String number         = account.getNumber();
    long   deviceId       = device.getId();
    String registrationId = device.getGcmId();

    gcmSender.sendMessage(number, deviceId, registrationId, pendingMessage);
  }

  private void sendApnMessage(Account account, Device device, PendingMessage outgoingMessage)
      throws TransientPushFailureException
  {
    apnSender.sendMessage(account, device, device.getApnId(), outgoingMessage);
  }

  private void sendWebSocketMessage(Account account, Device device, PendingMessage outgoingMessage)
  {
    webSocketSender.sendMessage(account, device, outgoingMessage);
  }
}
