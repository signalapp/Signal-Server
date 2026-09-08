package org.whispersystems.textsecuregcm.storage.foundationdb;

import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class FoundationDbUtil {

  public enum Context {
    INSERT_MESSAGE_BATCH("insertMessageBatch"),
    GET_MESSAGES_BATCH("getMessagesBatch"),
    SET_PRESENCE("setPresence"),
    GET_PRESENCE("getPresence"),
    CLEAR_PRESENCE("clearPresence"),
    GET_END_OF_QUEUE("getEndOfQueue"),
    ESTIMATE_QUEUE_SIZE("estimateQueueSize"),
    ESTIMATE_QUEUE_SIZE_AND_RANGE_SPLITS("estimateQueueSizeAndRangeSplits"),
    GET_RANGE_SPLITS("getRangeSplits"),
    TRIM_QUEUE("trimQueue"),
    DELETE_MESSAGE("deleteMessage");

    private final String name;

    Context(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  static final String TRANSACTION_ERRORS_COUNTER = MetricsUtil.name(FoundationDbUtil.class, "transactionErrors");
}
