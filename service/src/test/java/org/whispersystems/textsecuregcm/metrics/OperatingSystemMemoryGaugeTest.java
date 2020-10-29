/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class OperatingSystemMemoryGaugeTest {

    private static final String MEMINFO =
            "MemTotal:       16052208 kB\n" +
            "MemFree:         4568468 kB\n" +
            "MemAvailable:    7702848 kB\n" +
            "Buffers:          636372 kB\n" +
            "Cached:          5019116 kB\n" +
            "SwapCached:         6692 kB\n" +
            "Active:          7746436 kB\n" +
            "Inactive:        2729876 kB\n" +
            "Active(anon):    5580980 kB\n" +
            "Inactive(anon):  1648108 kB\n" +
            "Active(file):    2165456 kB\n" +
            "Inactive(file):  1081768 kB\n" +
            "Unevictable:      443948 kB\n" +
            "Mlocked:            4924 kB\n" +
            "SwapTotal:       1003516 kB\n" +
            "SwapFree:         935932 kB\n" +
            "Dirty:             28308 kB\n" +
            "Writeback:             0 kB\n" +
            "AnonPages:       5258396 kB\n" +
            "Mapped:          1530740 kB\n" +
            "Shmem:           2419340 kB\n" +
            "KReclaimable:     229392 kB\n" +
            "Slab:             408156 kB\n" +
            "SReclaimable:     229392 kB\n" +
            "SUnreclaim:       178764 kB\n" +
            "KernelStack:       17360 kB\n" +
            "PageTables:        50436 kB\n" +
            "NFS_Unstable:          0 kB\n" +
            "Bounce:                0 kB\n" +
            "WritebackTmp:          0 kB\n" +
            "CommitLimit:     9029620 kB\n" +
            "Committed_AS:   16681884 kB\n" +
            "VmallocTotal:   34359738367 kB\n" +
            "VmallocUsed:       41944 kB\n" +
            "VmallocChunk:          0 kB\n" +
            "Percpu:             4240 kB\n" +
            "HardwareCorrupted:     0 kB\n" +
            "AnonHugePages:         0 kB\n" +
            "ShmemHugePages:        0 kB\n" +
            "ShmemPmdMapped:        0 kB\n" +
            "FileHugePages:         0 kB\n" +
            "FilePmdMapped:         0 kB\n" +
            "CmaTotal:              0 kB\n" +
            "CmaFree:               0 kB\n" +
            "HugePages_Total:       0\n" +
            "HugePages_Free:        7\n" +
            "HugePages_Rsvd:        0\n" +
            "HugePages_Surp:        0\n" +
            "Hugepagesize:       2048 kB\n" +
            "Hugetlb:               0 kB\n" +
            "DirectMap4k:      481804 kB\n" +
            "DirectMap2M:    14901248 kB\n" +
            "DirectMap1G:     2097152 kB\n";

    @Test
    @Parameters(method = "argumentsForTestGetValue")
    public void testGetValue(final String metricName, final long expectedValue) {
        assertEquals(expectedValue, new OperatingSystemMemoryGauge(metricName).getValue(MEMINFO.lines()));
    }

    private static Object argumentsForTestGetValue() {
        return new Object[] {
                new Object[] { "MemTotal",       16052208L },
                new Object[] { "Active(anon)",   5580980L  },
                new Object[] { "Committed_AS",   16681884L },
                new Object[] { "HugePages_Free", 7L        },
                new Object[] { "NonsenseMetric", 0L        }
        };
    }
}
