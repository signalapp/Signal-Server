/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.currency;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

public class CurrencyConversionManager implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(CurrencyConversionManager.class);

  private static final Duration FIXER_REFRESH_INTERVAL = Duration.ofMinutes(15);
  @VisibleForTesting
  static final String FIXER_SHARED_CACHE_CURRENT_KEY = "CurrencyConversionManager::FixerCacheCurrent";
  private static final String FIXER_SHARED_CACHE_DATA_KEY = "CurrencyConversionManager::FixerCacheData";

  private static final Duration COIN_GECKO_REFRESH_INTERVAL = Duration.ofMinutes(5);
  @VisibleForTesting
  static final String COIN_GECKO_SHARED_CACHE_CURRENT_KEY = "CurrencyConversionManager::CoinGeckoCacheCurrent";
  private static final String COIN_GECKO_SHARED_CACHE_DATA_KEY = "CurrencyConversionManager::CoinGeckoCacheData";

  private static final String CACHED_DATA_UPDATED_COUNTER_NAME = MetricsUtil.name(CurrencyConversionManager.class, "cachedDataUpdate");
  private static final String SOURCE_TAG_NAME = "source";

  private static final Counter CACHED_DATA_UPDATE_ERRORS_COUNTER = Metrics.counter(
      MetricsUtil.name(CurrencyConversionManager.class, "errors"));

  private final FixerClient fixerClient;

  private final CoinGeckoClient coinGeckoClient;

  private final FaultTolerantRedisClusterClient cacheCluster;

  private final Clock clock;

  private final List<String> currencies;

  private final ScheduledExecutorService executor;

  private final AtomicReference<CurrencyConversionEntityList> cached = new AtomicReference<>(null);

  private Map<String, BigDecimal> cachedFixerValues;

  private Map<String, BigDecimal> cachedCoinGeckoValues;

  private ScheduledFuture<?> cacheUpdateFuture;

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
    cacheUpdateFuture = executor.scheduleWithFixedDelay(() -> {
      try {
        update();
      } catch (Throwable t) {
        CACHED_DATA_UPDATE_ERRORS_COUNTER.increment();
        logger.warn("Error updating currency conversions", t);
      }
    }, 0, 15, TimeUnit.SECONDS);
  }

  @Override
  public void stop() throws Exception {
    if (cacheUpdateFuture != null) {
      cacheUpdateFuture.cancel(true);
    }
  }

  @VisibleForTesting
  void update() throws IOException {
    updateFixerCacheIfNecessary();
    updateCoinGeckoCacheIfNecessary();
    updateEntity();
  }

  private void updateEntity() {
    final List<CurrencyConversionEntity> entities = new ArrayList<>(cachedCoinGeckoValues.size());

    for (Map.Entry<String, BigDecimal> currency : cachedCoinGeckoValues.entrySet()) {
      final BigDecimal usdValue = stripTrailingZerosAfterDecimal(currency.getValue());

      final Map<String, BigDecimal> values = new HashMap<>();
      values.put("USD", usdValue);

      for (Map.Entry<String, BigDecimal> conversion : cachedFixerValues.entrySet()) {
        values.put(conversion.getKey(), stripTrailingZerosAfterDecimal(conversion.getValue().multiply(usdValue)));
      }

      entities.add(new CurrencyConversionEntity(currency.getKey(), values));
    }

    this.cached.set(new CurrencyConversionEntityList(entities, clock.millis()));
  }

  private void updateFixerCacheIfNecessary() throws IOException {
    {
      final Map<String, BigDecimal> fixerValuesFromSharedCache = getCachedData(FIXER_SHARED_CACHE_DATA_KEY);

      if (fixerValuesFromSharedCache != null && !fixerValuesFromSharedCache.isEmpty()) {
        cachedFixerValues = fixerValuesFromSharedCache;
      }
    }

    final boolean shouldUpdateSharedCache = shouldUpdateSharedCache(FIXER_SHARED_CACHE_CURRENT_KEY,
        FIXER_REFRESH_INTERVAL);

    if (shouldUpdateSharedCache || cachedFixerValues == null) {

      cachedFixerValues = new HashMap<>(fixerClient.getConversionsForBase("USD"));

      if (shouldUpdateSharedCache) {
        updateCachedData(FIXER_SHARED_CACHE_DATA_KEY, cachedFixerValues);
        Metrics.counter(CACHED_DATA_UPDATED_COUNTER_NAME, SOURCE_TAG_NAME, "fixer").increment();
      }
    }
  }

  private void updateCoinGeckoCacheIfNecessary() throws IOException {
    {
      final Map<String, BigDecimal> coinGeckoValuesFromSharedCache = getCachedData(COIN_GECKO_SHARED_CACHE_DATA_KEY);

      if (coinGeckoValuesFromSharedCache != null && !coinGeckoValuesFromSharedCache.isEmpty()) {
        cachedCoinGeckoValues = coinGeckoValuesFromSharedCache;
      }
    }

    final boolean shouldUpdateSharedCache = shouldUpdateSharedCache(COIN_GECKO_SHARED_CACHE_CURRENT_KEY,
        COIN_GECKO_REFRESH_INTERVAL);

    if (shouldUpdateSharedCache || cachedCoinGeckoValues == null) {
      final Map<String, BigDecimal> conversionRatesFromCoinGecko = new HashMap<>(currencies.size());

      for (final String currency : currencies) {
        conversionRatesFromCoinGecko.put(currency, coinGeckoClient.getSpotPrice(currency, "USD"));
      }

      cachedCoinGeckoValues = conversionRatesFromCoinGecko;

      if (shouldUpdateSharedCache) {
        updateCachedData(COIN_GECKO_SHARED_CACHE_DATA_KEY, cachedCoinGeckoValues);
        Metrics.counter(CACHED_DATA_UPDATED_COUNTER_NAME, SOURCE_TAG_NAME, "coingecko").increment();
      }
    }
  }

  private Map<String, BigDecimal> getCachedData(final String cacheKey) {
    return cacheCluster.withCluster(connection -> {
      final Map<String, BigDecimal> parsedSharedCacheData = new HashMap<>();

      connection.sync().hgetall(cacheKey).forEach((currency, conversionRate) ->
          parsedSharedCacheData.put(currency, new BigDecimal(conversionRate)));

      return parsedSharedCacheData;
    });
  }

  private boolean shouldUpdateSharedCache(final String cacheKey, final Duration interval) {
    return cacheCluster.withCluster(connection ->
        "OK".equals(connection.sync().set(cacheKey, "true", SetArgs.Builder.nx().ex(interval))));
  }

  private void updateCachedData(final String cacheKey, final Map<String, BigDecimal> data) {
    cacheCluster.useCluster(connection -> {
      final Map<String, String> sharedValues = new HashMap<>();

      data.forEach((currency, conversionRate) -> sharedValues.put(currency, conversionRate.toString()));

      connection.sync().hset(cacheKey, sharedValues);
    });
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
