package org.whispersystems.textsecuregcm.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Pair;

import java.io.IOException;

public class NetworkSentGauge extends NetworkGauge {

  private final Logger logger = LoggerFactory.getLogger(NetworkSentGauge.class);

  private long lastTimestamp;
  private long lastSent;

  @Override
  public Long getValue() {
    try {
      long             timestamp       = System.currentTimeMillis();
      Pair<Long, Long> sentAndReceived = getSentReceived();
      long             result          = 0;

      if (lastTimestamp != 0) {
        result        = sentAndReceived.first() - lastSent;
        lastSent      = sentAndReceived.first();
      }

      lastTimestamp = timestamp;
      return result;
    } catch (IOException e) {
      logger.warn("NetworkSentGauge", e);
      return -1L;
    }
  }
}
