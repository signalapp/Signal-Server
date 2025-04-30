package org.whispersystems.textsecuregcm.grpc.net;

import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractLeakDetectionTest {

  private static ResourceLeakDetector.Level originalResourceLeakDetectorLevel;

  @BeforeAll
  static void setLeakDetectionLevel() {
    originalResourceLeakDetectorLevel = ResourceLeakDetector.getLevel();
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @AfterAll
  static void restoreLeakDetectionLevel() {
    ResourceLeakDetector.setLevel(originalResourceLeakDetectorLevel);
  }
}
