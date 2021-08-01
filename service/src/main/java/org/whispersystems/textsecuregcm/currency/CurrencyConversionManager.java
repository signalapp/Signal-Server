package org.whispersystems.textsecuregcm.currency;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.dropwizard.lifecycle.Managed;

public class CurrencyConversionManager implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(CurrencyConversionManager.class);

  private static final long FIXER_INTERVAL = TimeUnit.HOURS.toMillis(2);
  private static final long FTX_INTERVAL   = TimeUnit.MINUTES.toMillis(5);

  private final FixerClient  fixerClient;
  private final FtxClient    ftxClient;
  private final List<String> currencies;

  private AtomicReference<CurrencyConversionEntityList> cached = new AtomicReference<>(null);

  private long fixerUpdatedTimestamp;
  private long ftxUpdatedTimestamp;

  private Map<String, BigDecimal> cachedFixerValues;
  private Map<String, BigDecimal> cachedFtxValues;

  public CurrencyConversionManager(FixerClient fixerClient, FtxClient ftxClient, List<String> currencies) {
    this.fixerClient = fixerClient;
    this.ftxClient   = ftxClient;
    this.currencies  = currencies;
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
    if (System.currentTimeMillis() - fixerUpdatedTimestamp > FIXER_INTERVAL || cachedFixerValues == null) {
      this.cachedFixerValues     = new HashMap<>(fixerClient.getConversionsForBase("USD"));
      this.fixerUpdatedTimestamp = System.currentTimeMillis();
    }

    if (System.currentTimeMillis() - ftxUpdatedTimestamp > FTX_INTERVAL || cachedFtxValues == null) {
      Map<String, BigDecimal> cachedFtxValues = new HashMap<>();

      for (String currency : currencies) {
        cachedFtxValues.put(currency, ftxClient.getSpotPrice(currency, "USD"));
      }

      this.cachedFtxValues     = cachedFtxValues;
      this.ftxUpdatedTimestamp = System.currentTimeMillis();
    }

    List<CurrencyConversionEntity> entities = new LinkedList<>();

    for (Map.Entry<String, BigDecimal> currency : cachedFtxValues.entrySet()) {
      BigDecimal usdValue = stripTrailingZerosAfterDecimal(currency.getValue());

      Map<String, BigDecimal> values = new HashMap<>();
      values.put("USD", usdValue);

      for (Map.Entry<String, BigDecimal> conversion : cachedFixerValues.entrySet()) {
        values.put(conversion.getKey(), stripTrailingZerosAfterDecimal(conversion.getValue().multiply(usdValue)));
      }

      entities.add(new CurrencyConversionEntity(currency.getKey(), values));
    }


    this.cached.set(new CurrencyConversionEntityList(entities,  ftxUpdatedTimestamp));
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
  void setFixerUpdatedTimestamp(long timestamp) {
    this.fixerUpdatedTimestamp = timestamp;
  }

  @VisibleForTesting
  void setFtxUpdatedTimestamp(long timestamp) {
    this.ftxUpdatedTimestamp = timestamp;
  }

}
