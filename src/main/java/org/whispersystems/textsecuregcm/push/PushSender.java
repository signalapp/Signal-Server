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
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.Device;
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
  public void fillLocalAccountsCache(Map<String, Pair<Boolean, Set<Long>>> destinations, Map<Pair<String, Long>, Device> accountCache, List<String> numbersMissingDevices) {
    for (Map.Entry<String, Pair<Boolean, Set<Long>>> destination : destinations.entrySet()) {
      if (destination.getValue().first()) {
        String number = destination.getKey();
        List<Device> deviceList = accounts.getAllByNumber(number);
        Set<Long> deviceIdsIncluded = destination.getValue().second();
        if (deviceList.size() != deviceIdsIncluded.size())
          numbersMissingDevices.add(number);
        else {
          for (Device device : deviceList) {
            if (!deviceIdsIncluded.contains(device.getDeviceId())) {
              numbersMissingDevices.add(number);
              break;
            }
          }
          for (Device device : deviceList)
            accountCache.put(new Pair<>(number, device.getDeviceId()), device);
        }
      }
    }
  }

  public void sendMessage(Device device, MessageProtos.OutgoingMessageSignal outgoingMessage)
      throws IOException, NoSuchUserException
  {
    String signalingKey              = device.getSignalingKey();
    EncryptedOutgoingMessage message = new EncryptedOutgoingMessage(outgoingMessage, signalingKey);

    if      (device.getGcmRegistrationId() != null) sendGcmMessage(device, message);
    else if (device.getApnRegistrationId() != null) sendApnMessage(device, message);
    else if (device.getFetchesMessages())           storeFetchedMessage(device, message);
    else                                             throw new NoSuchUserException("No push identifier!");
  }

  private void sendGcmMessage(Device device, EncryptedOutgoingMessage outgoingMessage)
      throws IOException, NoSuchUserException
  {
    try {
      String canonicalId = gcmSender.sendMessage(device.getGcmRegistrationId(),
                                                 outgoingMessage);

      if (canonicalId != null) {
        device.setGcmRegistrationId(canonicalId);
        accounts.update(device);
      }

    } catch (NoSuchUserException e) {
      logger.debug("No Such User", e);
      device.setGcmRegistrationId(null);
      accounts.update(device);
      throw new NoSuchUserException("User no longer exists in GCM.");
    }
  }

  private void sendApnMessage(Device device, EncryptedOutgoingMessage outgoingMessage)
      throws IOException
  {
    apnSender.sendMessage(device.getApnRegistrationId(), outgoingMessage);
  }

  private void storeFetchedMessage(Device device, EncryptedOutgoingMessage outgoingMessage) throws IOException {
    storedMessageManager.storeMessage(device, outgoingMessage);
  }
}
