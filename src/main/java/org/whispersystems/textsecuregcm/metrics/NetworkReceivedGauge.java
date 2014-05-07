package org.whispersystems.textsecuregcm.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Pair;

import java.io.IOException;

public class NetworkReceivedGauge extends NetworkGauge {

  private final Logger logger = LoggerFactory.getLogger(NetworkSentGauge.class);

  private long lastTimestamp;
  private long lastReceived;

  @Override
  public Long getValue() {
    try {
      long             timestamp       = System.currentTimeMillis();
      Pair<Long, Long> sentAndReceived = getSentReceived();
      long             result          = 0;

      if (lastTimestamp != 0) {
        result       = sentAndReceived.second() - lastReceived;
        lastReceived = sentAndReceived.second();
      }

      lastTimestamp = timestamp;
      return result;
    } catch (IOException e) {
      logger.warn("NetworkReceivedGauge", e);
      return -1L;
    }
  }

}
