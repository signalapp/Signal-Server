package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.Map;

public class CurrencyConversionEntity {

  @JsonProperty
  private String base;

  @JsonProperty
  private Map<String, BigDecimal> conversions;

  public CurrencyConversionEntity(String base, Map<String, BigDecimal> conversions) {
    this.base        = base;
    this.conversions = conversions;
  }

  public CurrencyConversionEntity() {}

  public String getBase() {
    return base;
  }

  public Map<String, BigDecimal> getConversions() {
    return conversions;
  }

}
