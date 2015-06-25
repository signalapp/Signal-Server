package org.whispersystems.textsecuregcm.storage;

import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class Messages {

  private static final String ID                 = "id";
  private static final String TYPE               = "type";
  private static final String RELAY              = "relay";
  private static final String TIMESTAMP          = "timestamp";
  private static final String SOURCE             = "source";
  private static final String SOURCE_DEVICE      = "source_device";
  private static final String DESTINATION        = "destination";
  private static final String DESTINATION_DEVICE = "destination_device";
  private static final String MESSAGE            = "message";
  private static final String CONTENT            = "content";

  @SqlQuery("INSERT INTO messages (" + TYPE + ", " + RELAY + ", " + TIMESTAMP + ", " + SOURCE + ", " + SOURCE_DEVICE + ", " + DESTINATION + ", " + DESTINATION_DEVICE + ", " + MESSAGE + ", " + CONTENT + ") " +
            "VALUES (:type, :relay, :timestamp, :source, :source_device, :destination, :destination_device, :message, :content) " +
            "RETURNING (SELECT COUNT(id) FROM messages WHERE " + DESTINATION + " = :destination AND " + DESTINATION_DEVICE + " = :destination_device AND " + TYPE + " != " + Envelope.Type.RECEIPT_VALUE + ")")
  abstract int store(@MessageBinder Envelope message,
                     @Bind("destination") String destination,
                     @Bind("destination_device") long destinationDevice);

  @Mapper(MessageMapper.class)
  @SqlQuery("SELECT * FROM messages WHERE " + DESTINATION + " = :destination AND " + DESTINATION_DEVICE + " = :destination_device ORDER BY " + TIMESTAMP + " ASC")
  abstract List<OutgoingMessageEntity> load(@Bind("destination")        String destination,
                                            @Bind("destination_device") long destinationDevice);

  @Mapper(MessageMapper.class)
  @SqlQuery("DELETE FROM messages WHERE " + ID + " IN (SELECT " + ID + " FROM messages WHERE " + DESTINATION + " = :destination AND " + SOURCE + " = :source AND " + TIMESTAMP + " = :timestamp ORDER BY " + ID + " LIMIT 1) RETURNING *")
  abstract OutgoingMessageEntity remove(@Bind("destination") String destination, @Bind("source") String source, @Bind("timestamp") long timestamp);

  @Mapper(MessageMapper.class)
  @SqlUpdate("DELETE FROM messages WHERE " + ID + " = :id AND " + DESTINATION + " = :destination")
  abstract void remove(@Bind("destination") String destination, @Bind("id") long id);

  @SqlUpdate("DELETE FROM messages WHERE " + DESTINATION + " = :destination")
  abstract void clear(@Bind("destination") String destination);

  @SqlUpdate("VACUUM messages")
  public abstract void vacuum();

  public static class MessageMapper implements ResultSetMapper<OutgoingMessageEntity> {
    @Override
    public OutgoingMessageEntity map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {

      int    type          = resultSet.getInt(TYPE);
      byte[] legacyMessage = resultSet.getBytes(MESSAGE);

      if (type == Envelope.Type.RECEIPT_VALUE && legacyMessage == null) {
        /// XXX - REMOVE AFTER 10/01/15
        legacyMessage = new byte[0];
      }

      return new OutgoingMessageEntity(resultSet.getLong(ID),
                                       type,
                                       resultSet.getString(RELAY),
                                       resultSet.getLong(TIMESTAMP),
                                       resultSet.getString(SOURCE),
                                       resultSet.getInt(SOURCE_DEVICE),
                                       legacyMessage,
                                       resultSet.getBytes(CONTENT));
    }
  }

  @BindingAnnotation(MessageBinder.AccountBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  public @interface MessageBinder {
    public static class AccountBinderFactory implements BinderFactory {
      @Override
      public Binder build(Annotation annotation) {
        return new Binder<MessageBinder, Envelope>() {
          @Override
          public void bind(SQLStatement<?> sql,
                           MessageBinder accountBinder,
                           Envelope message)
          {
            sql.bind(TYPE, message.getType().getNumber());
            sql.bind(RELAY, message.getRelay());
            sql.bind(TIMESTAMP, message.getTimestamp());
            sql.bind(SOURCE, message.getSource());
            sql.bind(SOURCE_DEVICE, message.getSourceDevice());
            sql.bind(MESSAGE, message.hasLegacyMessage() ? message.getLegacyMessage().toByteArray() : null);
            sql.bind(CONTENT, message.hasContent() ? message.getContent().toByteArray() : null);
          }
        };
      }
    }
  }


}
