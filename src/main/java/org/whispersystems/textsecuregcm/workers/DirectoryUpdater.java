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
package org.whispersystems.textsecuregcm.workers;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.federation.FederatedClient;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.NumberData;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Iterator;
import java.util.List;

public class DirectoryUpdater {

  private final Logger logger = LoggerFactory.getLogger(DirectoryUpdater.class);

  private final AccountsManager        accountsManager;
  private final FederatedClientManager federatedClientManager;
  private final DirectoryManager       directory;

  public DirectoryUpdater(AccountsManager accountsManager,
                          FederatedClientManager federatedClientManager,
                          DirectoryManager directory)
  {
    this.accountsManager        = accountsManager;
    this.federatedClientManager = federatedClientManager;
    this.directory              = directory;
  }

  public void updateFromLocalDatabase() {
    BatchOperationHandle batchOperation = directory.startBatchOperation();

    try {
      Iterator<NumberData> numbers = accountsManager.getAllNumbers();

      if (numbers == null)
        return;

      while (numbers.hasNext()) {
        NumberData number = numbers.next();
        if (number.isActive()) {
          byte[]        token         = Util.getContactToken(number.getNumber());
          ClientContact clientContact = new ClientContact(token, null, number.isSupportsSms());

          directory.add(batchOperation, clientContact);

          logger.debug("Adding local token: " + Base64.encodeBytesWithoutPadding(token));
        } else {
          directory.remove(batchOperation, number.getNumber());
        }
      }
    } finally {
      directory.stopBatchOperation(batchOperation);
    }

    logger.info("Local directory is updated.");
  }

  public void updateFromPeers() {
    logger.info("Updating peer directories.");
    List<FederatedClient> clients = federatedClientManager.getClients();

    for (FederatedClient client : clients) {
      logger.info("Updating directory from peer: " + client.getPeerName());
      BatchOperationHandle handle = directory.startBatchOperation();

      try {
        int userCount = client.getUserCount();
        int retrieved = 0;

        logger.info("Remote peer user count: " + userCount);

        while (retrieved < userCount) {
          List<ClientContact> clientContacts = client.getUserTokens(retrieved);

          if (clientContacts == null)
            break;

          for (ClientContact clientContact : clientContacts) {
            clientContact.setRelay(client.getPeerName());

            Optional<ClientContact> existing = directory.get(clientContact.getToken());

            if (!clientContact.isInactive() && (!existing.isPresent() || existing.get().getRelay().equals(client.getPeerName()))) {
              directory.add(handle, clientContact);
            } else {
              if (existing != null && client.getPeerName().equals(existing.get().getRelay())) {
                directory.remove(clientContact.getToken());
              }
            }
          }

          retrieved += clientContacts.size();
        }

        logger.info("Update from peer complete.");
      } finally {
        directory.stopBatchOperation(handle);
      }
    }

    logger.info("Update from peer directories complete.");
  }
}
