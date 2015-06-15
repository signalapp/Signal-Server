package org.whispersystems.textsecuregcm.storage;


import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;

import java.util.List;

public class MessagesManager {

  private final Messages messages;

  public MessagesManager(Messages messages) {
    this.messages = messages;
  }

  public int insert(String destination, long destinationDevice, Envelope message) {
    return this.messages.store(message, destination, destinationDevice) + 1;
  }

  public List<OutgoingMessageEntity> getMessagesForDevice(String destination, long destinationDevice) {
    return this.messages.load(destination, destinationDevice);
  }

  public void clear(String destination) {
    this.messages.clear(destination);
  }

  public Optional<OutgoingMessageEntity> delete(String destination, String source, long timestamp) {
    return Optional.fromNullable(this.messages.remove(destination, source, timestamp));
  }

  public void delete(String destination, long id) {
    this.messages.remove(destination, id);
  }
}
