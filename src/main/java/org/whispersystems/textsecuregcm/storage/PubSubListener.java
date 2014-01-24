package org.whispersystems.textsecuregcm.storage;

public interface PubSubListener {

  public void onPubSubMessage(PubSubMessage outgoingMessage);

}
