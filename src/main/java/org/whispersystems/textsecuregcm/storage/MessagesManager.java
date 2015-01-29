package org.whispersystems.textsecuregcm.storage;


import org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.List;

public class MessagesManager {

  private final Messages messages;

  public MessagesManager(Messages messages) {
    this.messages = messages;
  }

  public int insert(String destination, long destinationDevice, OutgoingMessageSignal message) {
    return this.messages.store(message, destination, destinationDevice) + 1;
  }

  public List<Pair<Long, OutgoingMessageSignal>> getMessagesForDevice(String destination, long destinationDevice) {
    return this.messages.load(destination, destinationDevice);
  }

  public void clear(String destination) {
    this.messages.clear(destination);
  }

  public void delete(long id) {
    this.messages.remove(id);
  }
}
