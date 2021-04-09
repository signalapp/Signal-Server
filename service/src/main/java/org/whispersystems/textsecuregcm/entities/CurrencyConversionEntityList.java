package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CurrencyConversionEntityList {

  @JsonProperty
  private List<CurrencyConversionEntity> currencies;

  @JsonProperty
  private long timestamp;

  public CurrencyConversionEntityList(List<CurrencyConversionEntity> currencies, long timestamp) {
    this.currencies = currencies;
    this.timestamp  = timestamp;
  }

  public CurrencyConversionEntityList() {}

  public List<CurrencyConversionEntity> getCurrencies() {
    return currencies;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
