/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TurnCallRouterTest {

  private final static String TEST_HOSTNAME = "subdomain.example.org";
  private final static List<String> TEST_URLS_WITH_HOSTS = List.of(
      "stun:one.example.com",
      "turn:two.example.com",
      "turn:three.example.com?transport=tcp"
  );
  private final static List<String> EXPECTED_TEST_URLS_WITH_HOSTS = List.of(
      "turn:two.example.com"
  );

  private CallRoutingTable performanceTable;
  private CallRoutingTable manualTable;
  private DynamicConfigTurnRouter configTurnRouter;
  private DatabaseReader geoIp;
  private Country country;
  private Continent continent;
  private CallDnsRecords callDnsRecords;
  private Subdivision subdivision;
  private UUID aci = UUID.randomUUID();

  @BeforeEach
  void setup() throws IOException, GeoIp2Exception {
    performanceTable = mock(CallRoutingTable.class);
    manualTable = mock(CallRoutingTable.class);
    configTurnRouter = mock(DynamicConfigTurnRouter.class);
    geoIp = mock(DatabaseReader.class);
    continent = mock(Continent.class);
    country = mock(Country.class);
    subdivision = mock(Subdivision.class);
    ArrayList<Subdivision> subdivisions = new ArrayList<>();
    subdivisions.add(subdivision);

    when(geoIp.city(any())).thenReturn(new CityResponse(null, continent, country, null, null, null, null, null, subdivisions, null));
    setupDefault();
  }

  void setupDefault() {
    when(configTurnRouter.targetedUrls(any())).thenReturn(Collections.emptyList());
    when(configTurnRouter.randomUrls()).thenReturn(TEST_URLS_WITH_HOSTS);
    when(configTurnRouter.getHostname()).thenReturn(TEST_HOSTNAME);
    when(configTurnRouter.shouldRandomize()).thenReturn(false);
    when(manualTable.getDatacentersFor(any(), any(), any(), any())).thenReturn(Collections.emptyList());
    when(continent.getCode()).thenReturn("NA");
    when(country.getIsoCode()).thenReturn("US");
    when(subdivision.getIsoCode()).thenReturn("VA");
    try {
      callDnsRecords = new CallDnsRecords(
          Map.of(
              "dc-manual", List.of(InetAddress.getByName("1.1.1.1")),
              "dc-performance1", List.of(
                  InetAddress.getByName("9.9.9.1"),
                  InetAddress.getByName("9.9.9.2")
              ),
              "dc-performance2", List.of(InetAddress.getByName("9.9.9.3")),
              "dc-performance3", List.of(InetAddress.getByName("9.9.9.4")),
              "dc-performance4", List.of(
                  InetAddress.getByName("9.9.9.5"),
                  InetAddress.getByName("9.9.9.6"),
                  InetAddress.getByName("9.9.9.7")
              )
          ),
          Map.of(
              "dc-manual", List.of(InetAddress.getByName("2222:1111:0:dead::")),
              "dc-performance1", List.of(
                  InetAddress.getByName("2222:1111:0:abc0::"),
                  InetAddress.getByName("2222:1111:0:abc1::")
              ),
              "dc-performance2", List.of(InetAddress.getByName("2222:1111:0:abc2::")),
              "dc-performance3", List.of(InetAddress.getByName("2222:1111:0:abc3::")),
              "dc-performance4", List.of(
                  InetAddress.getByName("2222:1111:0:abc4::"),
                  InetAddress.getByName("2222:1111:0:abc5::"),
                  InetAddress.getByName("2222:1111:0:abc6::")
              )
          )
      );
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private TurnCallRouter router() {
    return new TurnCallRouter(
        () -> callDnsRecords,
        () -> performanceTable,
        () -> manualTable,
        configTurnRouter,
        () -> geoIp,
        // set to true so the return values are predictable
        true
    );
  }

  TurnServerOptions optionsWithUrls(List<String> urls) {
    return new TurnServerOptions(
        TEST_HOSTNAME,
        Optional.of(urls),
        Optional.of(EXPECTED_TEST_URLS_WITH_HOSTS)
    );
  }

  @Test
  public void testPrioritizesTargetedUrls() throws UnknownHostException {
    List<String> targetedUrls = List.of(
        "targeted1.example.com",
        "targeted.example.com"
    );
    when(configTurnRouter.targetedUrls(any()))
        .thenReturn(targetedUrls);

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(new TurnServerOptions(
            TEST_HOSTNAME,
            Optional.empty(),
            Optional.of(targetedUrls)
        ));
  }

  @Test
  public void testRandomizes() throws UnknownHostException {
    when(configTurnRouter.shouldRandomize())
        .thenReturn(true);

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(new TurnServerOptions(
            TEST_HOSTNAME,
            Optional.empty(),
            Optional.of(TEST_URLS_WITH_HOSTS)
        ));
  }

  @Test
  public void testUrlsOnlyNoInstanceIps() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-performance2", "dc-performance1"));
    when(configTurnRouter.shouldRandomize())
        .thenReturn(false);

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 0))
        .isEqualTo(new TurnServerOptions(
            TEST_HOSTNAME,
            Optional.empty(),
            Optional.of(TEST_URLS_WITH_HOSTS)
        ));
  }

  @Test
  public void testOrderedByPerformance() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-performance2", "dc-performance1"));

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(optionsWithUrls(List.of(
            "turn:9.9.9.3",
            "turn:9.9.9.3:80?transport=tcp",
            "turns:9.9.9.3:443?transport=tcp",

            "turn:9.9.9.1",
            "turn:9.9.9.1:80?transport=tcp",
            "turns:9.9.9.1:443?transport=tcp",

            "turn:9.9.9.2",
            "turn:9.9.9.2:80?transport=tcp",
            "turns:9.9.9.2:443?transport=tcp",

            "turn:[2222:1111:0:abc2:0:0:0:0]",
            "turn:[2222:1111:0:abc2:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc2:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc0:0:0:0:0]",
            "turn:[2222:1111:0:abc0:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc0:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc1:0:0:0:0]",
            "turn:[2222:1111:0:abc1:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc1:0:0:0:0]:443?transport=tcp"
        )));
  }

  @Test
  public void testPrioritizesManualRecords() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-performance1"));
    when(manualTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-manual"));

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(optionsWithUrls(List.of(
            "turn:1.1.1.1",
            "turn:1.1.1.1:80?transport=tcp",
            "turns:1.1.1.1:443?transport=tcp",

            "turn:[2222:1111:0:dead:0:0:0:0]",
            "turn:[2222:1111:0:dead:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:dead:0:0:0:0]:443?transport=tcp"
        )));
  }

  @Test
  public void testLimitPrioritizesBestDataCenters() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-performance3", "dc-performance2", "dc-performance3"));

    // gets one instance from best two datacenters
    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 2))
        .isEqualTo(optionsWithUrls(List.of(
            "turn:9.9.9.4",
            "turn:9.9.9.4:80?transport=tcp",
            "turns:9.9.9.4:443?transport=tcp",

            "turn:9.9.9.3",
            "turn:9.9.9.3:80?transport=tcp",
            "turns:9.9.9.3:443?transport=tcp",

            "turn:[2222:1111:0:abc3:0:0:0:0]",
            "turn:[2222:1111:0:abc3:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc3:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc2:0:0:0:0]",
            "turn:[2222:1111:0:abc2:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc2:0:0:0:0]:443?transport=tcp"
        )));

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("2222:1111:0:abc2:0:0:0:1")), 1))
        .isEqualTo(optionsWithUrls(List.of(
            "turn:9.9.9.4",
            "turn:9.9.9.4:80?transport=tcp",
            "turns:9.9.9.4:443?transport=tcp",

            "turn:[2222:1111:0:abc3:0:0:0:0]",
            "turn:[2222:1111:0:abc3:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc3:0:0:0:0]:443?transport=tcp"
        )));
  }

  @Test
  public void testBackFillsUpToLimit() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("dc-performance4", "dc-performance2", "dc-performance3"));

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 5))
        .isEqualTo(optionsWithUrls(List.of(
            "turn:9.9.9.5",
            "turn:9.9.9.5:80?transport=tcp",
            "turns:9.9.9.5:443?transport=tcp",

            "turn:9.9.9.6",
            "turn:9.9.9.6:80?transport=tcp",
            "turns:9.9.9.6:443?transport=tcp",

            "turn:9.9.9.7",
            "turn:9.9.9.7:80?transport=tcp",
            "turns:9.9.9.7:443?transport=tcp",

            "turn:9.9.9.3",
            "turn:9.9.9.3:80?transport=tcp",
            "turns:9.9.9.3:443?transport=tcp",

            "turn:9.9.9.4",
            "turn:9.9.9.4:80?transport=tcp",
            "turns:9.9.9.4:443?transport=tcp",

            "turn:[2222:1111:0:abc4:0:0:0:0]",
            "turn:[2222:1111:0:abc4:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc4:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc5:0:0:0:0]",
            "turn:[2222:1111:0:abc5:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc5:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc6:0:0:0:0]",
            "turn:[2222:1111:0:abc6:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc6:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc2:0:0:0:0]",
            "turn:[2222:1111:0:abc2:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc2:0:0:0:0]:443?transport=tcp",

            "turn:[2222:1111:0:abc3:0:0:0:0]",
            "turn:[2222:1111:0:abc3:0:0:0:0]:80?transport=tcp",
            "turns:[2222:1111:0:abc3:0:0:0:0]:443?transport=tcp"
        )));
  }

  @Test
  public void testNoDatacentersMatched() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of());

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(optionsWithUrls(List.of()));
  }

  @Test
  public void testHandlesDatacenterNotInDnsRecords() throws UnknownHostException {
    when(performanceTable.getDatacentersFor(any(), any(), any(), any()))
        .thenReturn(List.of("unsynced-datacenter"));

    assertThat(router().getRoutingFor(aci, Optional.of(InetAddress.getByName("0.0.0.1")), 10))
        .isEqualTo(optionsWithUrls(List.of()));
  }
}
