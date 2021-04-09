package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class CurrencyConversionEntity {

  @JsonProperty
  private String base;

  @JsonProperty
  private Map<String, Double> conversions;

  public CurrencyConversionEntity(String base, Map<String, Double> conversions) {
    this.base        = base;
    this.conversions = conversions;
  }

  public CurrencyConversionEntity() {}

  public String getBase() {
    return base;
  }

  public Map<String, Double> getConversions() {
    return conversions;
  }

}
