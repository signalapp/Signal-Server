package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;

public interface PubSubListener {

  public void onPubSubMessage(PubSubMessage outgoingMessage);

}
