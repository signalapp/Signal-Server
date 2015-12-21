package org.whispersystems.textsecuregcm.storage;


import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;

import java.util.List;

public class MessagesManager {

  private final Messages messages;

  public MessagesManager(Messages messages) {
    this.messages = messages;
  }

  public int insert(String destination, long destinationDevice, Envelope message) {
    return this.messages.store(message, destination, destinationDevice) + 1;
  }

  public OutgoingMessageEntityList getMessagesForDevice(String destination, long destinationDevice) {
    List<OutgoingMessageEntity> messages = this.messages.load(destination, destinationDevice);
    return new OutgoingMessageEntityList(messages, messages.size() >= Messages.RESULT_SET_CHUNK_SIZE);
  }

  public void clear(String destination) {
    this.messages.clear(destination);
  }

  public void clear(String destination, long deviceId) {
    this.messages.clear(destination, deviceId);
  }

  public Optional<OutgoingMessageEntity> delete(String destination, long destinationDevice, String source, long timestamp)
  {
    return Optional.fromNullable(this.messages.remove(destination, destinationDevice, source, timestamp));
  }

  public void delete(String destination, long id) {
    this.messages.remove(destination, id);
  }
}
