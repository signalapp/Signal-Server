/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CallRoutingTableParserTest {

  @Test
  public void testParserSuccess() throws IOException {
    var input =
        """
        192.1.12.0/24  datacenter-1 datacenter-2 datacenter-3
        193.123.123.0/24  datacenter-1  datacenter-2
        1.123.123.0/24  datacenter-1
        
        2001:db8:b0aa::/48  datacenter-1
        2001:db8:b0ab::/48  datacenter-3  datacenter-1 datacenter-2
        2001:db8:b0ac::/48  datacenter-2  datacenter-1
        
        SA-SR-v4 datacenter-3
        SA-UY-v4 datacenter-3 datacenter-1 datacenter-2
        NA-US-VA-v6 datacenter-2 datacenter-1
        """;
    var actual = CallRoutingTableParser.fromTsv(new StringReader(input));
    var expected = new CallRoutingTable(
        Map.of(
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-1")
        ),
        Map.of(
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ab::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-2", "datacenter-1")
        ),
        Map.of(
            new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
            new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
        )
    );

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testParserVariousWhitespaceSuccess() throws IOException {
    var input =
        """
        
            192.1.12.0/24\t  \tdatacenter-1\t\t datacenter-2      datacenter-3
        \t193.123.123.0/24\tdatacenter-1\tdatacenter-2
        
        
        1.123.123.0/24\t                datacenter-1
        2001:db8:b0aa::/48\t \tdatacenter-1
        2001:db8:b0ab::/48     \tdatacenter-3\tdatacenter-1          datacenter-2
        2001:db8:b0ac::/48\tdatacenter-2\tdatacenter-1
        
        
        
        
        
        
        SA-SR-v4 datacenter-3
        
        
        
        
        SA-UY-v4\tdatacenter-3\tdatacenter-1\tdatacenter-2
        NA-US-VA-v6              datacenter-2             \tdatacenter-1
        """;
    var actual = CallRoutingTableParser.fromTsv(new StringReader(input));
    var expected = new CallRoutingTable(
        Map.of(
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-1")
        ),
        Map.of(
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ab::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-2", "datacenter-1")
        ),
        Map.of(
            new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
            new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
        )
    );

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testParserMissingSection() throws IOException {
    var input =
        """
        192.1.12.0/24\t  \tdatacenter-1\t\t datacenter-2      datacenter-3
        193.123.123.0/24\tdatacenter-1\tdatacenter-2
        1.123.123.0/24\t                datacenter-1
        
        SA-SR-v4 datacenter-3
        SA-UY-v4\tdatacenter-3\tdatacenter-1\tdatacenter-2
        NA-US-VA-v6              datacenter-2             \tdatacenter-1
        """;
    var actual = CallRoutingTableParser.fromTsv(new StringReader(input));
    var expected = new CallRoutingTable(
        Map.of(
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-1")
        ),
        Map.of(),
        Map.of(
            new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
            new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
        )
    );

    assertThat(actual).isEqualTo(expected);
  }


  @Test
  public void testParserMixedSections() throws IOException {
    var input =
        """
        
        
        1.123.123.0/24\t                datacenter-1
        2001:db8:b0aa::/48\t \tdatacenter-1
        2001:db8:b0ab::/48     \tdatacenter-3\tdatacenter-1          datacenter-2
        2001:db8:b0ac::/48\tdatacenter-2\tdatacenter-1
        
        
        
            192.1.12.0/24\t  \tdatacenter-1\t\t datacenter-2      datacenter-3
            193.123.123.0/24\tdatacenter-1\tdatacenter-2
                    
        
        
        SA-SR-v4 datacenter-3
        
        
        
        
        SA-UY-v4\tdatacenter-3\tdatacenter-1\tdatacenter-2
        NA-US-VA-v6              datacenter-2             \tdatacenter-1
        """;
    var actual = CallRoutingTableParser.fromTsv(new StringReader(input));
    var expected = new CallRoutingTable(
        Map.of(
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-1"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2")
        ),
        Map.of(
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ab::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-2", "datacenter-1")
        ),
        Map.of(
            new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
            new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
        )
    );

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testJsonParserSuccess() throws IOException {
    var input =
        """
            {
            	"ipv4GeoToDataCenters": {
            		"SA-SR": ["datacenter-3"],
            		"SA-UY": ["datacenter-3", "datacenter-1", "datacenter-2"]
            	},
            	"ipv6GeoToDataCenters": {
            		"NA-US-VA": ["datacenter-2", "datacenter-1"]
            	},
            	"ipv4SubnetsToDatacenters": {
            		"192.1.12.0": ["datacenter-1", "datacenter-2", "datacenter-3"],
            		"193.123.123.0": ["datacenter-1", "datacenter-2"],
            		"1.123.123.0": ["datacenter-1"]
            	},
            	"ipv6SubnetsToDatacenters": {
            		"2001:db8:b0aa::": ["datacenter-1"],
            		"2001:db8:b0ab::": ["datacenter-3", "datacenter-1", "datacenter-2"],
            		"2001:db8:b0ac::": ["datacenter-2", "datacenter-1"]
            	}
            }
            """;
    var actual = CallRoutingTableParser.fromJson(new StringReader(input));
    var expected = new CallRoutingTable(
        Map.of(
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("192.1.12.0/24"), List.of("datacenter-1", "datacenter-2", "datacenter-3"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("193.123.123.0/24"), List.of("datacenter-1", "datacenter-2"),
            (CidrBlock.IpV4CidrBlock) CidrBlock.parseCidrBlock("1.123.123.0/24"), List.of("datacenter-1")
        ),
        Map.of(
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0aa::/48"), List.of("datacenter-1"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ab::/48"), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            (CidrBlock.IpV6CidrBlock) CidrBlock.parseCidrBlock("2001:db8:b0ac::/48"), List.of("datacenter-2", "datacenter-1")
        ),
        Map.of(
            new CallRoutingTable.GeoKey("SA", "SR", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3"),
            new CallRoutingTable.GeoKey("SA", "UY", Optional.empty(), CallRoutingTable.Protocol.v4), List.of("datacenter-3", "datacenter-1", "datacenter-2"),
            new CallRoutingTable.GeoKey("NA", "US", Optional.of("VA"), CallRoutingTable.Protocol.v6), List.of("datacenter-2", "datacenter-1")
        )
    );

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testParseVariousEdgeCases() throws IOException {
    var input =
        """
            {
            	"ipv4GeoToDataCenters": {},
            	"ipv6GeoToDataCenters": {},
            	"ipv4SubnetsToDatacenters": {},
            	"ipv6SubnetsToDatacenters": {}
            }
        """;
    assertThat(CallRoutingTableParser.fromJson(new StringReader(input))).isEqualTo(CallRoutingTable.empty());
    assertThat(CallRoutingTableParser.fromJson(new StringReader("{}"))).isEqualTo(CallRoutingTable.empty());
  }
}
