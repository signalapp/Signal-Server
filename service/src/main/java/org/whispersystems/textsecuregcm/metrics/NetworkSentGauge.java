package org.whispersystems.textsecuregcm.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Pair;

import java.io.IOException;

public class NetworkSentGauge extends NetworkGauge {

  private final Logger logger = LoggerFactory.getLogger(NetworkSentGauge.class);

  private long lastTimestamp;
  private long lastSent;

  public NetworkSentGauge() {
    try {
      this.lastTimestamp = System.currentTimeMillis();
      this.lastSent      = getSentReceived().first();
    } catch (IOException e) {
      logger.warn(NetworkSentGauge.class.getSimpleName(), e);
    }
  }

  @Override
  public Double getValue() {
    try {
      long             timestamp        = System.currentTimeMillis();
      Pair<Long, Long> sentAndReceived  = getSentReceived();
      double           bytesTransmitted = sentAndReceived.first() - lastSent;
      double           secondsElapsed   = (timestamp - this.lastTimestamp) / 1000;
      double           result           = bytesTransmitted / secondsElapsed;

      this.lastSent      = sentAndReceived.first();
      this.lastTimestamp = timestamp;

      return result;
    } catch (IOException e) {
      logger.warn("NetworkSentGauge", e);
      return -1D;
    }
  }
}
