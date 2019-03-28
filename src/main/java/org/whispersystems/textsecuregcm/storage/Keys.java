/*
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PreKey;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public abstract class Keys {

  private static final Logger logger = LoggerFactory.getLogger(Keys.class);

  @SqlUpdate("DELETE FROM keys WHERE number = :number AND device_id = :device_id")
  abstract void remove(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlBatch("INSERT INTO keys (number, device_id, key_id, public_key) VALUES (:number, :device_id, :key_id, :public_key)")
  abstract void append(@KeyRecordBinder List<KeyRecord> preKeys);

  @SqlQuery("DELETE FROM keys WHERE id IN (SELECT id FROM keys WHERE number = :number AND device_id = :device_id ORDER BY key_id ASC LIMIT 1) RETURNING *")
  @Mapper(KeyRecordMapper.class)
  abstract List<KeyRecord> getInternal(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlQuery("DELETE FROM keys WHERE id IN (SELECT DISTINCT ON (number, device_id) id FROM keys WHERE number = :number ORDER BY number, device_id, key_id ASC) RETURNING *")
  @Mapper(KeyRecordMapper.class)
  abstract List<KeyRecord> getInternal(@Bind("number") String number);

  @SqlQuery("SELECT COUNT(*) FROM keys WHERE number = :number AND device_id = :device_id")
  public abstract int getCount(@Bind("number") String number, @Bind("device_id") long deviceId);

  // Apparently transaction annotations don't work on the annotated query methods
  @SuppressWarnings("WeakerAccess")
  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  List<KeyRecord> getInternalWithTransaction(String number) {
    return getInternal(number);
  }

  // Apparently transaction annotations don't work on the annotated query methods
  @SuppressWarnings("WeakerAccess")
  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  List<KeyRecord> getInternalWithTransaction(String number, long deviceId) {
    return getInternal(number, deviceId);
  }

  public List<KeyRecord> get(String number) {
    return executeAndRetrySerializableAction(() -> getInternalWithTransaction(number));
  }

  public List<KeyRecord> get(String number, long deviceId) {
    return executeAndRetrySerializableAction(() -> getInternalWithTransaction(number, deviceId));
  }

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public void store(String number, long deviceId, List<PreKey> keys) {
    List<KeyRecord> records = keys.stream()
                                  .map(key -> new KeyRecord(0, number, deviceId, key.getKeyId(), key.getPublicKey()))
                                  .collect(Collectors.toList());

    remove(number, deviceId);
    append(records);
  }

  private List<KeyRecord> executeAndRetrySerializableAction(Callable<List<KeyRecord>> action) {
    for (int i=0;i<20;i++) {
      try {
        return action.call();
      } catch (UnableToExecuteStatementException e) {
        logger.info("Serializable conflict, retrying: " + e.getMessage());
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    throw new UnableToExecuteStatementException("Retried statement too many times!");
  }


  @SqlUpdate("VACUUM keys")
  public abstract void vacuum();

  @BindingAnnotation(KeyRecordBinder.PreKeyBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  public @interface KeyRecordBinder {
    public static class PreKeyBinderFactory implements BinderFactory {
      @Override
      public Binder build(Annotation annotation) {
        return new Binder<KeyRecordBinder, KeyRecord>() {
          @Override
          public void bind(SQLStatement<?> sql, KeyRecordBinder keyRecordBinder, KeyRecord record)
          {
            sql.bind("id", record.getId());
            sql.bind("number", record.getNumber());
            sql.bind("device_id", record.getDeviceId());
            sql.bind("key_id", record.getKeyId());
            sql.bind("public_key", record.getPublicKey());
          }
        };
      }
    }
  }


  public static class KeyRecordMapper implements ResultSetMapper<KeyRecord> {
    @Override
    public KeyRecord map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      return new KeyRecord(resultSet.getLong("id"), resultSet.getString("number"),
                           resultSet.getLong("device_id"), resultSet.getLong("key_id"),
                           resultSet.getString("public_key"));
    }
  }

}
