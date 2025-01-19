/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.currency;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.SetArgs;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

public class CurrencyConversionManager implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(CurrencyConversionManager.class);

  @VisibleForTesting
  static final Duration FIXER_REFRESH_INTERVAL = Duration.ofHours(2);

  private static final Duration COIN_GECKO_CAP_REFRESH_INTERVAL = Duration.ofMinutes(5);

  @VisibleForTesting
  static final String COIN_GECKO_CAP_SHARED_CACHE_CURRENT_KEY = "CurrencyConversionManager::CoinGeckoCacheCurrent";

  private static final String COIN_GECKO_SHARED_CACHE_DATA_KEY = "CurrencyConversionManager::CoinGeckoCacheData";

  private final FixerClient fixerClient;

  private final CoinGeckoClient coinGeckoClient;

  private final FaultTolerantRedisClusterClient cacheCluster;

  private final Clock clock;

  private final List<String> currencies;

  private final ScheduledExecutorService executor;

  private final AtomicReference<CurrencyConversionEntityList> cached = new AtomicReference<>(null);

  private Instant fixerUpdatedTimestamp = Instant.MIN;

  private Map<String, BigDecimal> cachedFixerValues;

  private Map<String, BigDecimal> cachedCoinGeckoValues;


  public CurrencyConversionManager(
      final FixerClient fixerClient,
      final CoinGeckoClient coinGeckoClient,
      final FaultTolerantRedisClusterClient cacheCluster,
      final List<String> currencies,
      final ScheduledExecutorService executor,
      final Clock clock) {
    this.fixerClient = fixerClient;
    this.coinGeckoClient = coinGeckoClient;
    this.cacheCluster = cacheCluster;
    this.currencies  = currencies;
    this.executor = executor;
    this.clock = clock;
  }

  public Optional<CurrencyConversionEntityList> getCurrencyConversions() {
    return Optional.ofNullable(cached.get());
  }

  @Override
  public void start() throws Exception {
    executor.scheduleAtFixedRate(() -> {
      try {
        updateCacheIfNecessary();
      } catch (Throwable t) {
        logger.warn("Error updating currency conversions", t);
      }
    }, 0, 15, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  void updateCacheIfNecessary() throws IOException {
    if (Duration.between(fixerUpdatedTimestamp, clock.instant()).abs().compareTo(FIXER_REFRESH_INTERVAL) >= 0 || cachedFixerValues == null) {
      this.cachedFixerValues = new HashMap<>(fixerClient.getConversionsForBase("USD"));
      this.fixerUpdatedTimestamp = clock.instant();
    }

    {
      final Map<String, BigDecimal> CoinGeckoValuesFromSharedCache = cacheCluster.withCluster(connection -> {
        final Map<String, BigDecimal> parsedSharedCacheData = new HashMap<>();

        connection.sync().hgetall(COIN_GECKO_SHARED_CACHE_DATA_KEY).forEach((currency, conversionRate) ->
            parsedSharedCacheData.put(currency, new BigDecimal(conversionRate)));

        return parsedSharedCacheData;
      });

      if (CoinGeckoValuesFromSharedCache != null && !CoinGeckoValuesFromSharedCache.isEmpty()) {
        cachedCoinGeckoValues = CoinGeckoValuesFromSharedCache;
      }
    }

    final boolean shouldUpdateSharedCache = cacheCluster.withCluster(connection ->
        "OK".equals(connection.sync().set(COIN_GECKO_CAP_SHARED_CACHE_CURRENT_KEY,
            "true",
            SetArgs.Builder.nx().ex(COIN_GECKO_CAP_REFRESH_INTERVAL))));

    if (shouldUpdateSharedCache || cachedCoinGeckoValues == null) {
      final Map<String, BigDecimal> conversionRatesFromCoinGecko = new HashMap<>(currencies.size());

      for (final String currency : currencies) {
        conversionRatesFromCoinGecko.put(currency, coinGeckoClient.getSpotPrice(currency, "USD"));
      }

      cachedCoinGeckoValues = conversionRatesFromCoinGecko;

      if (shouldUpdateSharedCache) {
        cacheCluster.useCluster(connection -> {
          final Map<String, String> sharedCoinGeckoValues = new HashMap<>();

          cachedCoinGeckoValues.forEach((currency, conversionRate) ->
              sharedCoinGeckoValues.put(currency, conversionRate.toString()));

          connection.sync().hset(COIN_GECKO_SHARED_CACHE_DATA_KEY, sharedCoinGeckoValues);
        });
      }
    }

    List<CurrencyConversionEntity> entities = new LinkedList<>();

    for (Map.Entry<String, BigDecimal> currency : cachedCoinGeckoValues.entrySet()) {
      BigDecimal usdValue = stripTrailingZerosAfterDecimal(currency.getValue());

      Map<String, BigDecimal> values = new HashMap<>();
      values.put("USD", usdValue);

      for (Map.Entry<String, BigDecimal> conversion : cachedFixerValues.entrySet()) {
        values.put(conversion.getKey(), stripTrailingZerosAfterDecimal(conversion.getValue().multiply(usdValue)));
      }

      entities.add(new CurrencyConversionEntity(currency.getKey(), values));
    }

    this.cached.set(new CurrencyConversionEntityList(entities, clock.millis()));
  }

  private BigDecimal stripTrailingZerosAfterDecimal(BigDecimal bigDecimal) {
    BigDecimal n = bigDecimal.stripTrailingZeros();
    if (n.scale() < 0) {
      return n.setScale(0);
    } else {
      return n;
    }
  }

  @VisibleForTesting
  void setCachedFixerValues(final Map<String, BigDecimal> cachedFixerValues) {
    this.cachedFixerValues = cachedFixerValues;
  }

  public Optional<BigDecimal> convertToUsd(final BigDecimal amount, final String currency) {
    if ("USD".equalsIgnoreCase(currency)) {
      return Optional.of(amount);
    }

    return Optional.ofNullable(cachedFixerValues.get(currency.toUpperCase(Locale.ROOT)))
        .map(conversionRate -> amount.divide(conversionRate, 2, RoundingMode.HALF_EVEN));
  }
}
