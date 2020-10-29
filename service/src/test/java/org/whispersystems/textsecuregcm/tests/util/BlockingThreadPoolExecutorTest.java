/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.junit.Test;
import org.whispersystems.textsecuregcm.util.BlockingThreadPoolExecutor;
import org.whispersystems.textsecuregcm.util.Util;

import static org.junit.Assert.assertTrue;

public class BlockingThreadPoolExecutorTest {

  @Test
  public void testBlocking() {
    BlockingThreadPoolExecutor executor = new BlockingThreadPoolExecutor("test", 1, 3);
    long                       start    = System.currentTimeMillis();

    executor.execute(new Runnable() {
      @Override
      public void run() {
        Util.sleep(1000);
      }
    });

    assertTrue(System.currentTimeMillis() - start < 500);
    start = System.currentTimeMillis();

    executor.execute(new Runnable() {
      @Override
      public void run() {
        Util.sleep(1000);
      }
    });

    assertTrue(System.currentTimeMillis() - start < 500);

    start = System.currentTimeMillis();

    executor.execute(new Runnable() {
      @Override
      public void run() {
        Util.sleep(1000);
      }
    });

    assertTrue(System.currentTimeMillis() - start < 500);

    start = System.currentTimeMillis();

    executor.execute(new Runnable() {
      @Override
      public void run() {
        Util.sleep(1000);
      }
    });

    assertTrue(System.currentTimeMillis() - start > 500);
  }

}
