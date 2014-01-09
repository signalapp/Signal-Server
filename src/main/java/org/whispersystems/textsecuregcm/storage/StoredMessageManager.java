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
package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;

import java.io.IOException;
import java.util.List;

public class StoredMessageManager {
  StoredMessages storedMessages;
  public StoredMessageManager(StoredMessages storedMessages) {
    this.storedMessages = storedMessages;
  }

  public void storeMessage(Account account, EncryptedOutgoingMessage outgoingMessage) throws IOException {
    storedMessages.insert(account.getId(), outgoingMessage.serialize());
  }

  public List<String> getStoredMessage(Account account) {
    return storedMessages.getMessagesForAccountId(account.getId());
  }
}
