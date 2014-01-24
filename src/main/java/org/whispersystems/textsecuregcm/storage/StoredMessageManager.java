/**
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
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.List;

public class StoredMessageManager {

  private final StoredMessages storedMessages;
  private final PubSubManager  pubSubManager;

  public StoredMessageManager(StoredMessages storedMessages, PubSubManager pubSubManager) {
    this.storedMessages = storedMessages;
    this.pubSubManager  = pubSubManager;
  }

  public void storeMessage(Account account, Device device, EncryptedOutgoingMessage outgoingMessage)
      throws CryptoEncodingException
  {
    storeMessage(account, device, outgoingMessage.serialize());
  }

  public void storeMessages(Account account, Device device, List<String> serializedMessages) {
    for (String serializedMessage : serializedMessages) {
      storeMessage(account, device, serializedMessage);
    }
  }

  private void storeMessage(Account account, Device device, String serializedMessage) {
    if (device.getFetchesMessages()) {
      WebsocketAddress address       = new WebsocketAddress(account.getId(), device.getId());
      PubSubMessage    pubSubMessage = new PubSubMessage(PubSubMessage.TYPE_DELIVER, serializedMessage);

      if (!pubSubManager.publish(address, pubSubMessage)) {
        storedMessages.insert(account.getId(), device.getId(), serializedMessage);
        pubSubManager.publish(address, new PubSubMessage(PubSubMessage.TYPE_QUERY_DB, null));
      }

      return;
    }

    storedMessages.insert(account.getId(), device.getId(), serializedMessage);
  }

  public List<String> getOutgoingMessages(Account account, Device device) {
    return storedMessages.getMessagesForDevice(account.getId(), device.getId());
  }
}
