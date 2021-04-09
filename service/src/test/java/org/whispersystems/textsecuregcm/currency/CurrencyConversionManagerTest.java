package org.whispersystems.textsecuregcm.currency;

import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CurrencyConversionManagerTest {

  @Test
  public void testCurrencyCalculations() throws IOException {
    FixerClient fixerClient = mock(FixerClient.class);
    FtxClient   ftxClient   = mock(FtxClient.class);

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(2.35);
    when(fixerClient.getConversionsForBase(eq("USD"))).thenReturn(Map.of("EUR", 0.822876, "FJD", 2.0577,"FKP", 0.743446));

    CurrencyConversionManager manager = new CurrencyConversionManager(fixerClient, ftxClient, List.of("FOO"));

    manager.updateCacheIfNecessary();

    CurrencyConversionEntityList conversions = manager.getCurrencyConversions().orElseThrow();

    assertThat(conversions.getCurrencies().size()).isEqualTo(1);
    assertThat(conversions.getCurrencies().get(0).getBase()).isEqualTo("FOO");
    assertThat(conversions.getCurrencies().get(0).getConversions().size()).isEqualTo(4);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("USD")).isEqualTo(2.35);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("EUR")).isEqualTo(1.9337586000000002);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FJD")).isEqualTo(4.8355950000000005);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FKP")).isEqualTo(1.7470981);
  }

  @Test
  public void testCurrencyCalculationsTimeoutNoRun() throws IOException {
    FixerClient fixerClient = mock(FixerClient.class);
    FtxClient   ftxClient   = mock(FtxClient.class);

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(2.35);
    when(fixerClient.getConversionsForBase(eq("USD"))).thenReturn(Map.of("EUR", 0.822876, "FJD", 2.0577,"FKP", 0.743446));

    CurrencyConversionManager manager = new CurrencyConversionManager(fixerClient, ftxClient, List.of("FOO"));

    manager.updateCacheIfNecessary();

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(3.50);

    manager.updateCacheIfNecessary();

    CurrencyConversionEntityList conversions = manager.getCurrencyConversions().orElseThrow();

    assertThat(conversions.getCurrencies().size()).isEqualTo(1);
    assertThat(conversions.getCurrencies().get(0).getBase()).isEqualTo("FOO");
    assertThat(conversions.getCurrencies().get(0).getConversions().size()).isEqualTo(4);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("USD")).isEqualTo(2.35);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("EUR")).isEqualTo(1.9337586000000002);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FJD")).isEqualTo(4.8355950000000005);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FKP")).isEqualTo(1.7470981);
  }

  @Test
  public void testCurrencyCalculationsFtxTimeoutWithRun() throws IOException {
    FixerClient fixerClient = mock(FixerClient.class);
    FtxClient   ftxClient   = mock(FtxClient.class);

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(2.35);
    when(fixerClient.getConversionsForBase(eq("USD"))).thenReturn(Map.of("EUR", 0.822876, "FJD", 2.0577,"FKP", 0.743446));

    CurrencyConversionManager manager = new CurrencyConversionManager(fixerClient, ftxClient, List.of("FOO"));

    manager.updateCacheIfNecessary();

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(3.50);
    manager.setFtxUpdatedTimestamp(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2) - TimeUnit.SECONDS.toMillis(1));
    manager.updateCacheIfNecessary();

    CurrencyConversionEntityList conversions = manager.getCurrencyConversions().orElseThrow();

    assertThat(conversions.getCurrencies().size()).isEqualTo(1);
    assertThat(conversions.getCurrencies().get(0).getBase()).isEqualTo("FOO");
    assertThat(conversions.getCurrencies().get(0).getConversions().size()).isEqualTo(4);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("USD")).isEqualTo(3.5);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("EUR")).isEqualTo(2.8800660000000002);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FJD")).isEqualTo(7.20195);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FKP")).isEqualTo(2.602061);
  }


  @Test
  public void testCurrencyCalculationsFixerTimeoutWithRun() throws IOException {
    FixerClient fixerClient = mock(FixerClient.class);
    FtxClient   ftxClient   = mock(FtxClient.class);

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(2.35);
    when(fixerClient.getConversionsForBase(eq("USD"))).thenReturn(Map.of("EUR", 0.822876, "FJD", 2.0577,"FKP", 0.743446));

    CurrencyConversionManager manager = new CurrencyConversionManager(fixerClient, ftxClient, List.of("FOO"));

    manager.updateCacheIfNecessary();

    when(ftxClient.getSpotPrice(eq("FOO"), eq("USD"))).thenReturn(3.50);
    when(fixerClient.getConversionsForBase(eq("USD"))).thenReturn(Map.of("EUR", 0.922876, "FJD", 2.0577,"FKP", 0.743446));

    manager.setFixerUpdatedTimestamp(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2) - TimeUnit.SECONDS.toMillis(1));
    manager.updateCacheIfNecessary();

    CurrencyConversionEntityList conversions = manager.getCurrencyConversions().orElseThrow();

    assertThat(conversions.getCurrencies().size()).isEqualTo(1);
    assertThat(conversions.getCurrencies().get(0).getBase()).isEqualTo("FOO");
    assertThat(conversions.getCurrencies().get(0).getConversions().size()).isEqualTo(4);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("USD")).isEqualTo(2.35);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("EUR")).isEqualTo(2.1687586000000003);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FJD")).isEqualTo(4.8355950000000005);
    assertThat(conversions.getCurrencies().get(0).getConversions().get("FKP")).isEqualTo(1.7470981);

  }

}
