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
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.configuration.GcmConfiguration;
import org.whispersystems.textsecuregcm.entities.CryptoEncodingException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.StoredMessageManager;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class PushSender {

  private final Logger logger = LoggerFactory.getLogger(PushSender.class);

  private final AccountsManager      accounts;
  private final GCMSender            gcmSender;
  private final APNSender            apnSender;
  private final StoredMessageManager storedMessageManager;

  public PushSender(GcmConfiguration gcmConfiguration,
                    ApnConfiguration apnConfiguration,
                    StoredMessageManager storedMessageManager,
                    AccountsManager accounts)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException
  {
    this.accounts             = accounts;
    this.storedMessageManager = storedMessageManager;
    this.gcmSender            = new GCMSender(gcmConfiguration.getApiKey());
    this.apnSender            = new APNSender(apnConfiguration.getCertificate(), apnConfiguration.getKey());
  }

  public void sendMessage(Account account, Device device, MessageProtos.OutgoingMessageSignal outgoingMessage)
      throws NotPushRegisteredException, TransientPushFailureException
  {
    String                   signalingKey = device.getSignalingKey();
    EncryptedOutgoingMessage message      = new EncryptedOutgoingMessage(outgoingMessage, signalingKey);

    if      (device.getGcmId() != null)   sendGcmMessage(account, device, message);
    else if (device.getApnId() != null)   sendApnMessage(account, device, message);
    else if (device.getFetchesMessages()) storeFetchedMessage(device, message);
    else                                  throw new NotPushRegisteredException("No delivery possible!");
  }

  private void sendGcmMessage(Account account, Device device, EncryptedOutgoingMessage outgoingMessage)
      throws NotPushRegisteredException, TransientPushFailureException
  {
    try {
      String canonicalId = gcmSender.sendMessage(device.getGcmId(), outgoingMessage);

      if (canonicalId != null) {
        device.setGcmId(canonicalId);
        accounts.update(account);
      }

    } catch (NotPushRegisteredException e) {
      logger.debug("No Such User", e);
      device.setGcmId(null);
      accounts.update(account);
      throw new NotPushRegisteredException(e);
    }
  }

  private void sendApnMessage(Account account, Device device, EncryptedOutgoingMessage outgoingMessage)
      throws TransientPushFailureException, NotPushRegisteredException
  {
    try {
      apnSender.sendMessage(device.getApnId(), outgoingMessage);
    } catch (NotPushRegisteredException e) {
      device.setApnId(null);
      accounts.update(account);
      throw new NotPushRegisteredException(e);
    }
  }

  private void storeFetchedMessage(Device device, EncryptedOutgoingMessage outgoingMessage)
      throws NotPushRegisteredException
  {
    try {
      storedMessageManager.storeMessage(device, outgoingMessage);
    } catch (CryptoEncodingException e) {
      throw new NotPushRegisteredException(e);
    }
  }
}
