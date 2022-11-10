package org.whispersystems.textsecuregcm.currency;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.SetArgs;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Util;

public class CurrencyConversionManager implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(CurrencyConversionManager.class);

  @VisibleForTesting
  static final Duration FIXER_REFRESH_INTERVAL = Duration.ofHours(2);

  private static final Duration COIN_MARKET_CAP_REFRESH_INTERVAL = Duration.ofMinutes(5);

  @VisibleForTesting
  static final String COIN_MARKET_CAP_SHARED_CACHE_CURRENT_KEY = "CurrencyConversionManager::CoinMarketCapCacheCurrent";
  private static final String COIN_MARKET_CAP_SHARED_CACHE_DATA_KEY = "CurrencyConversionManager::CoinMarketCapCacheData";

  private final FixerClient  fixerClient;
  private final CoinMarketCapClient coinMarketCapClient;
  private final FaultTolerantRedisCluster cacheCluster;
  private final Clock clock;

  private final List<String> currencies;

  private final AtomicReference<CurrencyConversionEntityList> cached = new AtomicReference<>(null);

  private Instant fixerUpdatedTimestamp = Instant.MIN;

  private Map<String, BigDecimal> cachedFixerValues;
  private Map<String, BigDecimal> cachedCoinMarketCapValues;

  public CurrencyConversionManager(final FixerClient fixerClient,
      final CoinMarketCapClient coinMarketCapClient,
      final FaultTolerantRedisCluster cacheCluster,
      final List<String> currencies,
      final Clock clock) {
    this.fixerClient = fixerClient;
    this.coinMarketCapClient = coinMarketCapClient;
    this.cacheCluster = cacheCluster;
    this.currencies  = currencies;
    this.clock = clock;
  }

  public Optional<CurrencyConversionEntityList> getCurrencyConversions() {
    return Optional.ofNullable(cached.get());
  }

  @Override
  public void start() throws Exception {
    new Thread(() -> {
      for (;;) {
        try {
          updateCacheIfNecessary();
        } catch (Throwable t) {
          logger.warn("Error updating currency conversions", t);
        }

        Util.sleep(15000);
      }
    }).start();
  }

  @Override
  public void stop() throws Exception {

  }

  @VisibleForTesting
  void updateCacheIfNecessary() throws IOException {
    if (Duration.between(fixerUpdatedTimestamp, clock.instant()).abs().compareTo(FIXER_REFRESH_INTERVAL) >= 0 || cachedFixerValues == null) {
      this.cachedFixerValues = new HashMap<>(fixerClient.getConversionsForBase("USD"));
      this.fixerUpdatedTimestamp = clock.instant();
    }

    {
      final Map<String, BigDecimal> coinMarketCapValuesFromSharedCache = cacheCluster.withCluster(connection -> {
        final Map<String, BigDecimal> parsedSharedCacheData = new HashMap<>();

        connection.sync().hgetall(COIN_MARKET_CAP_SHARED_CACHE_DATA_KEY).forEach((currency, conversionRate) ->
            parsedSharedCacheData.put(currency, new BigDecimal(conversionRate)));

        return parsedSharedCacheData;
      });

      if (coinMarketCapValuesFromSharedCache != null && !coinMarketCapValuesFromSharedCache.isEmpty()) {
        cachedCoinMarketCapValues = coinMarketCapValuesFromSharedCache;
      }
    }

    final boolean shouldUpdateSharedCache = cacheCluster.withCluster(connection ->
        "OK".equals(connection.sync().set(COIN_MARKET_CAP_SHARED_CACHE_CURRENT_KEY,
            "true",
            SetArgs.Builder.nx().ex(COIN_MARKET_CAP_REFRESH_INTERVAL))));

    if (shouldUpdateSharedCache || cachedCoinMarketCapValues == null) {
      final Map<String, BigDecimal> conversionRatesFromCoinMarketCap = new HashMap<>(currencies.size());

      for (final String currency : currencies) {
        conversionRatesFromCoinMarketCap.put(currency, coinMarketCapClient.getSpotPrice(currency, "USD"));
      }

      cachedCoinMarketCapValues = conversionRatesFromCoinMarketCap;

      if (shouldUpdateSharedCache) {
        cacheCluster.useCluster(connection -> {
          final Map<String, String> sharedCoinMarketCapValues = new HashMap<>();

          cachedCoinMarketCapValues.forEach((currency, conversionRate) ->
              sharedCoinMarketCapValues.put(currency, conversionRate.toString()));

          connection.sync().hset(COIN_MARKET_CAP_SHARED_CACHE_DATA_KEY, sharedCoinMarketCapValues);
        });
      }
    }

    List<CurrencyConversionEntity> entities = new LinkedList<>();

    for (Map.Entry<String, BigDecimal> currency : cachedCoinMarketCapValues.entrySet()) {
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
}
