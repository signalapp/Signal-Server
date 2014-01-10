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

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcmConfiguration;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.StoredMessageManager;
import org.whispersystems.textsecuregcm.util.Pair;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PushSender {

  private final Logger logger = LoggerFactory.getLogger(PushSender.class);

  private final AccountsManager  accounts;

  private final GCMSender gcmSender;
  private final APNSender apnSender;
  private final StoredMessageManager storedMessageManager;

  public PushSender(GcmConfiguration gcmConfiguration,
                    ApnConfiguration apnConfiguration,
                    StoredMessageManager storedMessageManager,
                    AccountsManager accounts,
                    DirectoryManager directory)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException
  {
    this.accounts  = accounts;

    this.storedMessageManager = storedMessageManager;
    this.gcmSender            = new GCMSender(gcmConfiguration.getApiKey());
    this.apnSender            = new APNSender(apnConfiguration.getCertificate(), apnConfiguration.getKey());
  }

  /**
   * For each local destination in destinations, either adds all its accounts to accountCache or adds the number to
   * numbersMissingDevices, if the deviceIds list don't match what is required.
   * @param destinations Map from number to Pair&lt;localNumber, Set&lt;deviceIds&gt;&gt;
   * @param accountCache Map from &lt;number, deviceId&gt; to account
   * @param numbersMissingDevices list of numbers missing devices
   */
  public void fillLocalAccountsCache(Map<String, Pair<Boolean, Set<Long>>> destinations, Map<Pair<String, Long>, Account> accountCache, List<String> numbersMissingDevices) {
    for (Map.Entry<String, Pair<Boolean, Set<Long>>> destination : destinations.entrySet()) {
      if (destination.getValue().first()) {
        String number = destination.getKey();
        List<Account> accountList = accounts.getAllByNumber(number);
        Set<Long> deviceIdsIncluded = destination.getValue().second();
        if (accountList.size() != deviceIdsIncluded.size())
          numbersMissingDevices.add(number);
        else {
          for (Account account : accountList) {
            if (!deviceIdsIncluded.contains(account.getDeviceId())) {
              numbersMissingDevices.add(number);
              break;
            }
          }
          for (Account account : accountList)
            accountCache.put(new Pair<>(number, account.getDeviceId()), account);
        }
      }
    }
  }

  public void sendMessage(Account account, MessageProtos.OutgoingMessageSignal outgoingMessage)
      throws IOException, NoSuchUserException
  {
    String signalingKey              = account.getSignalingKey();
    EncryptedOutgoingMessage message = new EncryptedOutgoingMessage(outgoingMessage, signalingKey);

    if      (account.getGcmRegistrationId() != null) sendGcmMessage(account, message);
    else if (account.getApnRegistrationId() != null) sendApnMessage(account, message);
    else if (account.getFetchesMessages())           storeFetchedMessage(account, message);
    else                                             throw new NoSuchUserException("No push identifier!");
  }

  private void sendGcmMessage(Account account, EncryptedOutgoingMessage outgoingMessage)
      throws IOException, NoSuchUserException
  {
    try {
      String canonicalId = gcmSender.sendMessage(account.getGcmRegistrationId(),
                                                 outgoingMessage);

      if (canonicalId != null) {
        account.setGcmRegistrationId(canonicalId);
        accounts.update(account);
      }

    } catch (NoSuchUserException e) {
      logger.debug("No Such User", e);
      account.setGcmRegistrationId(null);
      accounts.update(account);
      throw new NoSuchUserException("User no longer exists in GCM.");
    }
  }

  private void sendApnMessage(Account account, EncryptedOutgoingMessage outgoingMessage)
      throws IOException
  {
    apnSender.sendMessage(account.getApnRegistrationId(), outgoingMessage);
  }

  private void storeFetchedMessage(Account account, EncryptedOutgoingMessage outgoingMessage) throws IOException {
    storedMessageManager.storeMessage(account, outgoingMessage);
  }
}
