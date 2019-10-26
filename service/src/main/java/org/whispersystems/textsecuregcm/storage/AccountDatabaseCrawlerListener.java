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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;

import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class AccountDatabaseCrawlerListener {

  private Timer processChunkTimer;

  abstract public void onCrawlStart();

  abstract public void onCrawlEnd(Optional<UUID> fromUuid);

  abstract protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) throws AccountDatabaseCrawlerRestartException;

  public AccountDatabaseCrawlerListener() {
    processChunkTimer = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME).timer(name(AccountDatabaseCrawlerListener.class, "processChunk", getClass().getSimpleName()));
  }

  public void timeAndProcessCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) throws AccountDatabaseCrawlerRestartException {
    try (Timer.Context timer = processChunkTimer.time()) {
      onCrawlChunk(fromUuid, chunkAccounts);
    }
  }

}
