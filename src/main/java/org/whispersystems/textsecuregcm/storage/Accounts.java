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

import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public abstract class Accounts {

  public static final String ID               = "id";
  public static final String NUMBER           = "number";
  public static final String DEVICE_ID        = "device_id";
  public static final String AUTH_TOKEN       = "auth_token";
  public static final String SALT             = "salt";
  public static final String SIGNALING_KEY    = "signaling_key";
  public static final String GCM_ID           = "gcm_id";
  public static final String APN_ID           = "apn_id";
  public static final String FETCHES_MESSAGES = "fetches_messages";
  public static final String SUPPORTS_SMS     = "supports_sms";

  @SqlUpdate("INSERT INTO accounts (" + NUMBER + ", " + DEVICE_ID + ", " + AUTH_TOKEN + ", " +
                                    SALT + ", " + SIGNALING_KEY + ", " + FETCHES_MESSAGES + ", " +
                                    GCM_ID + ", " + APN_ID + ", " + SUPPORTS_SMS + ") " +
             "VALUES (:number, :device_id, :auth_token, :salt, :signaling_key, :fetches_messages, :gcm_id, :apn_id, :supports_sms)")
  @GetGeneratedKeys
  abstract long insertStep(@AccountBinder Device device);

  @SqlQuery("SELECT " + DEVICE_ID + " FROM accounts WHERE " + NUMBER + " = :number ORDER BY " + DEVICE_ID + " DESC LIMIT 1 FOR UPDATE")
  abstract long getHighestDeviceId(@Bind("number") String number);

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public long insert(@AccountBinder Device device) {
    device.setDeviceId(getHighestDeviceId(device.getNumber()) + 1);
    return insertStep(device);
  }

  @SqlUpdate("DELETE FROM accounts WHERE " + NUMBER + " = :number RETURNING id")
  abstract void removeAccountsByNumber(@Bind("number") String number);

  @SqlUpdate("UPDATE accounts SET " + AUTH_TOKEN + " = :auth_token, " + SALT + " = :salt, " +
             SIGNALING_KEY + " = :signaling_key, " + GCM_ID + " = :gcm_id, " + APN_ID + " = :apn_id, " +
             FETCHES_MESSAGES + " = :fetches_messages, " + SUPPORTS_SMS + " = :supports_sms " +
             "WHERE " + NUMBER + " = :number AND " + DEVICE_ID + " = :device_id")
  abstract void update(@AccountBinder Device device);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number AND " + DEVICE_ID + " = :device_id")
  abstract Device get(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlQuery("SELECT COUNT(DISTINCT " + NUMBER + ") from accounts")
  abstract long getNumberCount();

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + DEVICE_ID + " = 1 OFFSET :offset LIMIT :limit")
  abstract List<Device> getAllFirstAccounts(@Bind("offset") int offset, @Bind("limit") int length);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + DEVICE_ID + " = 1")
  public abstract Iterator<Device> getAllFirstAccounts();

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number")
  public abstract List<Device> getAllByNumber(@Bind("number") String number);

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public long insertClearingNumber(Device device) {
    removeAccountsByNumber(device.getNumber());
    device.setDeviceId(getHighestDeviceId(device.getNumber()) + 1);
    return insertStep(device);
  }

  public static class AccountMapper implements ResultSetMapper<Device> {

    @Override
    public Device map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      return new Device(resultSet.getLong(ID), resultSet.getString(NUMBER), resultSet.getLong(DEVICE_ID),
                         resultSet.getString(AUTH_TOKEN), resultSet.getString(SALT),
                         resultSet.getString(SIGNALING_KEY), resultSet.getString(GCM_ID),
                         resultSet.getString(APN_ID),
                         resultSet.getInt(SUPPORTS_SMS) == 1, resultSet.getInt(FETCHES_MESSAGES) == 1);
    }
  }

  @BindingAnnotation(AccountBinder.AccountBinderFactory.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER})
  public @interface AccountBinder {
    public static class AccountBinderFactory implements BinderFactory {
      @Override
      public Binder build(Annotation annotation) {
        return new Binder<AccountBinder, Device>() {
          @Override
          public void bind(SQLStatement<?> sql,
                           AccountBinder accountBinder,
                           Device device)
          {
            sql.bind(ID, device.getId());
            sql.bind(NUMBER, device.getNumber());
            sql.bind(DEVICE_ID, device.getDeviceId());
            sql.bind(AUTH_TOKEN, device.getAuthenticationCredentials()
                                        .getHashedAuthenticationToken());
            sql.bind(SALT, device.getAuthenticationCredentials().getSalt());
            sql.bind(SIGNALING_KEY, device.getSignalingKey());
            sql.bind(GCM_ID, device.getGcmRegistrationId());
            sql.bind(APN_ID, device.getApnRegistrationId());
            sql.bind(SUPPORTS_SMS, device.getSupportsSms() ? 1 : 0);
            sql.bind(FETCHES_MESSAGES, device.getFetchesMessages() ? 1 : 0);
          }
        };
      }
    }
  }

}
