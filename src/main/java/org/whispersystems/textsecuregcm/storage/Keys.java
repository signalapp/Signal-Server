/**
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

import com.google.common.base.Optional;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionIsolationLevel;
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
import org.whispersystems.textsecuregcm.entities.PreKey;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public abstract class Keys {

  @SqlUpdate("DELETE FROM keys WHERE number = :number AND device_id = :device_id")
  abstract void removeKeys(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlUpdate("DELETE FROM keys WHERE id = :id")
  abstract void removeKey(@Bind("id") long id);

  @SqlBatch("INSERT INTO keys (number, device_id, key_id, public_key, last_resort) VALUES " +
            "(:number, :device_id, :key_id, :public_key, :last_resort)")
  abstract void append(@PreKeyBinder List<KeyRecord> preKeys);

  @SqlQuery("SELECT * FROM keys WHERE number = :number AND device_id = :device_id ORDER BY key_id ASC FOR UPDATE")
  @Mapper(PreKeyMapper.class)
  abstract KeyRecord retrieveFirst(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlQuery("SELECT DISTINCT ON (number, device_id) * FROM keys WHERE number = :number ORDER BY number, device_id, key_id ASC")
  @Mapper(PreKeyMapper.class)
  abstract List<KeyRecord> retrieveFirst(@Bind("number") String number);

  @SqlQuery("SELECT COUNT(*) FROM keys WHERE number = :number AND device_id = :device_id")
  public abstract int getCount(@Bind("number") String number, @Bind("device_id") long deviceId);

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public void store(String number, long deviceId, List<PreKey> keys) {
    List<KeyRecord> records = new LinkedList<>();

    for (PreKey key : keys) {
      records.add(new KeyRecord(0, number, deviceId, key.getKeyId(), key.getPublicKey(), false));
    }

    removeKeys(number, deviceId);
    append(records);
  }

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public Optional<List<KeyRecord>> get(String number, long deviceId) {
    final KeyRecord record = retrieveFirst(number, deviceId);

    if (record != null && !record.isLastResort()) {
      removeKey(record.getId());
    } else if (record == null) {
      return Optional.absent();
    }

    List<KeyRecord> results = new LinkedList<>();
    results.add(record);

    return Optional.of(results);
  }

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public Optional<List<KeyRecord>> get(String number) {
    List<KeyRecord> preKeys = retrieveFirst(number);

    if (preKeys != null) {
      for (KeyRecord preKey : preKeys) {
        if (!preKey.isLastResort()) {
          removeKey(preKey.getId());
        }
      }
    }

    if (preKeys != null) return Optional.of(preKeys);
    else                 return Optional.absent();
  }

  @SqlUpdate("VACUUM keys")
  public abstract void vacuum();

  @BindingAnnotation(PreKeyBinder.PreKeyBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  public @interface PreKeyBinder {
    public static class PreKeyBinderFactory implements BinderFactory {
      @Override
      public Binder build(Annotation annotation) {
        return new Binder<PreKeyBinder, KeyRecord>() {
          @Override
          public void bind(SQLStatement<?> sql, PreKeyBinder accountBinder, KeyRecord record)
          {
            sql.bind("id", record.getId());
            sql.bind("number", record.getNumber());
            sql.bind("device_id", record.getDeviceId());
            sql.bind("key_id", record.getKeyId());
            sql.bind("public_key", record.getPublicKey());
            sql.bind("last_resort", record.isLastResort() ? 1 : 0);
          }
        };
      }
    }
  }


  public static class PreKeyMapper implements ResultSetMapper<KeyRecord> {
    @Override
    public KeyRecord map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      return new KeyRecord(resultSet.getLong("id"), resultSet.getString("number"),
                           resultSet.getLong("device_id"), resultSet.getLong("key_id"),
                           resultSet.getString("public_key"), resultSet.getInt("last_resort") == 1);
    }
  }

}
