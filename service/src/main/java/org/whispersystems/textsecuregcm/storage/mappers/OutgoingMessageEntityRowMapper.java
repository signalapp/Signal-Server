package org.whispersystems.textsecuregcm.storage.mappers;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.Messages;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class OutgoingMessageEntityRowMapper implements RowMapper<OutgoingMessageEntity> {
  @Override
  public OutgoingMessageEntity map(ResultSet resultSet, StatementContext ctx) throws SQLException {
    int    type          = resultSet.getInt(Messages.TYPE);
    byte[] legacyMessage = resultSet.getBytes(Messages.MESSAGE);
    String guid          = resultSet.getString(Messages.GUID);
    String sourceUuid    = resultSet.getString(Messages.SOURCE_UUID);

    if (type == Envelope.Type.RECEIPT_VALUE && legacyMessage == null) {
      /// XXX - REMOVE AFTER 10/01/15
      legacyMessage = new byte[0];
    }

    return new OutgoingMessageEntity(resultSet.getLong(Messages.ID),
                                     false,
                                     guid == null ? null : UUID.fromString(guid),
                                     type,
                                     resultSet.getString(Messages.RELAY),
                                     resultSet.getLong(Messages.TIMESTAMP),
                                     resultSet.getString(Messages.SOURCE),
                                     sourceUuid == null ? null : UUID.fromString(sourceUuid),
                                     resultSet.getInt(Messages.SOURCE_DEVICE),
                                     legacyMessage,
                                     resultSet.getBytes(Messages.CONTENT),
                                     resultSet.getLong(Messages.SERVER_TIMESTAMP));
  }
}
