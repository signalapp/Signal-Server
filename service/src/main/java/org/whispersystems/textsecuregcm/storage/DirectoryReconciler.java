/*
 * Copyright (C) 2018 Open WhisperSystems
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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.ProcessingException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements AccountDatabaseCrawlerListener {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private final DirectoryManager              directoryManager;
  private final DirectoryReconciliationClient reconciliationClient;

  public DirectoryReconciler(DirectoryReconciliationClient reconciliationClient, DirectoryManager directoryManager) {
    this.directoryManager     = directoryManager;
    this.reconciliationClient = reconciliationClient;
  }

  public void onCrawlStart() { }

  public void onCrawlEnd(Optional<String> fromNumber) {

    DirectoryReconciliationRequest  request  = new DirectoryReconciliationRequest(fromNumber.orElse(null), null, Collections.emptyList());
    DirectoryReconciliationResponse response = sendChunk(request);

  }

  public void onCrawlChunk(Optional<String> fromNumber, List<Account> chunkAccounts) throws AccountDatabaseCrawlerRestartException {

    updateDirectoryCache(chunkAccounts);

    DirectoryReconciliationRequest  request  = createChunkRequest(fromNumber, chunkAccounts);
    DirectoryReconciliationResponse response = sendChunk(request);
    if (response.getStatus() == DirectoryReconciliationResponse.Status.MISSING) {
      throw new AccountDatabaseCrawlerRestartException("directory reconciler missing");
    }
  }

  private void updateDirectoryCache(List<Account> accounts) {

    BatchOperationHandle batchOperation = directoryManager.startBatchOperation();

    try {
      for (Account account : accounts) {
        if (account.isActive()) {
          byte[]        token         = Util.getContactToken(account.getNumber());
          ClientContact clientContact = new ClientContact(token, null, true, true);
          directoryManager.add(batchOperation, clientContact);
        } else {
          directoryManager.remove(batchOperation, account.getNumber());
        }
      }
    } finally {
      directoryManager.stopBatchOperation(batchOperation);
    }
  }

  private DirectoryReconciliationRequest createChunkRequest(Optional<String> fromNumber, List<Account> accounts) {
    List<String> numbers = accounts.stream()
                                   .filter(Account::isActive)
                                   .map(Account::getNumber)
                                   .collect(Collectors.toList());

    Optional<String> toNumber   = Optional.empty();

    if (!accounts.isEmpty()) {
      toNumber   = Optional.of(accounts.get(accounts.size() - 1).getNumber());
    }

    return new DirectoryReconciliationRequest(fromNumber.orElse(null), toNumber.orElse(null), numbers);
  }

  private DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    try (Timer.Context timer = sendChunkTimer.time()) {
      DirectoryReconciliationResponse response = reconciliationClient.sendChunk(request);
      if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
        sendChunkErrorMeter.mark();
        logger.warn("reconciliation error: " + response.getStatus());
      }
      return response;
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      throw new ProcessingException(ex);
    }
  }

}
