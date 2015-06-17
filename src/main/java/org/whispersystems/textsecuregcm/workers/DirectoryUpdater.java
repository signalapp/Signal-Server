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
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.whispersystems.textsecuregcm.storage.DirectoryManager.PendingClientContact;

public class DirectoryUpdater {

  private static final int CHUNK_SIZE = 10000;

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
    int                  contactsAdded   = 0;
    int                  contactsRemoved = 0;
    BatchOperationHandle batchOperation  = directory.startBatchOperation();

    try {
      logger.info("Updating from local DB.");
      int offset = 0;

      for (;;) {
        List<Account> accounts = accountsManager.getAll(offset, CHUNK_SIZE);

        if (accounts == null || accounts.isEmpty()) break;
        else                                        offset += accounts.size();

        for (Account account : accounts) {
          if (account.isActive()) {
            byte[]        token         = Util.getContactToken(account.getNumber());
            ClientContact clientContact = new ClientContact(token, null);

            directory.add(batchOperation, clientContact);
            contactsAdded++;
          } else {
            directory.remove(batchOperation, account.getNumber());
            contactsRemoved++;
          }
        }

        logger.info("Processed " + CHUNK_SIZE + " local accounts...");
      }
    } finally {
      directory.stopBatchOperation(batchOperation);
    }

    logger.info(String.format("Local directory is updated (%d added, %d removed).", contactsAdded, contactsRemoved));
  }

  public void updateFromPeers() {
    logger.info("Updating peer directories.");

    int                   contactsAdded   = 0;
    int                   contactsRemoved = 0;
    List<FederatedClient> clients         = federatedClientManager.getClients();

    for (FederatedClient client : clients) {
      logger.info("Updating directory from peer: " + client.getPeerName());

      int userCount = client.getUserCount();
      int retrieved = 0;

      logger.info("Remote peer user count: " + userCount);

      while (retrieved < userCount) {
        logger.info("Retrieving remote tokens...");
        List<ClientContact>        remoteContacts = client.getUserTokens(retrieved);
        List<PendingClientContact> localContacts  = new LinkedList<>();
        BatchOperationHandle       handle         = directory.startBatchOperation();

        if (remoteContacts == null) {
          logger.info("Remote tokens empty, ending...");
          break;
        } else {
          logger.info("Retrieved " + remoteContacts.size() + " remote tokens...");
        }

        for (ClientContact remoteContact : remoteContacts) {
          localContacts.add(directory.get(handle, remoteContact.getToken()));
        }

        directory.stopBatchOperation(handle);

        handle = directory.startBatchOperation();
        Iterator<ClientContact>        remoteContactIterator = remoteContacts.iterator();
        Iterator<PendingClientContact> localContactIterator  = localContacts.iterator();

        while (remoteContactIterator.hasNext() && localContactIterator.hasNext()) {
          try {
            ClientContact           remoteContact = remoteContactIterator.next();
            Optional<ClientContact> localContact  = localContactIterator.next().get();

            remoteContact.setRelay(client.getPeerName());

            if (!remoteContact.isInactive() && (!localContact.isPresent() || client.getPeerName().equals(localContact.get().getRelay()))) {
              contactsAdded++;
              directory.add(handle, remoteContact);
            } else {
              if (localContact.isPresent() && client.getPeerName().equals(localContact.get().getRelay())) {
                contactsRemoved++;
                directory.remove(handle, remoteContact.getToken());
              }
            }
          } catch (IOException e) {
            logger.warn("JSON Serialization Failed: ", e);
          }
        }

        directory.stopBatchOperation(handle);

        retrieved += remoteContacts.size();
        logger.info("Processed: " + retrieved + " remote tokens.");
      }

      logger.info("Update from peer complete.");
    }

    logger.info("Update from peer directories complete.");
    logger.info(String.format("Added %d and removed %d remove contacts.", contactsAdded, contactsRemoved));
  }
}
