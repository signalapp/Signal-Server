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
  abstract long insertStep(@AccountBinder Account account);

  @SqlQuery("SELECT " + DEVICE_ID + " FROM accounts WHERE " + NUMBER + " = :number ORDER BY " + DEVICE_ID + " DESC LIMIT 1 FOR UPDATE")
  abstract long getHighestDeviceId(@Bind("number") String number);

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public long insert(@AccountBinder Account account) {
    account.setDeviceId(getHighestDeviceId(account.getNumber()) + 1);
    return insertStep(account);
  }

  @SqlUpdate("DELETE FROM accounts WHERE " + NUMBER + " = :number RETURNING id")
  abstract void removeAccountsByNumber(@Bind("number") String number);

  @SqlUpdate("UPDATE accounts SET " + AUTH_TOKEN + " = :auth_token, " + SALT + " = :salt, " +
             SIGNALING_KEY + " = :signaling_key, " + GCM_ID + " = :gcm_id, " + APN_ID + " = :apn_id, " +
             FETCHES_MESSAGES + " = :fetches_messages, " + SUPPORTS_SMS + " = :supports_sms " +
             "WHERE " + NUMBER + " = :number AND " + DEVICE_ID + " = :device_id")
  abstract void update(@AccountBinder Account account);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number AND " + DEVICE_ID + " = :device_id")
  abstract Account get(@Bind("number") String number, @Bind("device_id") long deviceId);

  @SqlQuery("SELECT COUNT(DISTINCT " + NUMBER + ") from accounts")
  abstract long getNumberCount();

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + DEVICE_ID + " = 1 OFFSET :offset LIMIT :limit")
  abstract List<Account> getAllFirstAccounts(@Bind("offset") int offset, @Bind("limit") int length);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + DEVICE_ID + " = 1")
  public abstract Iterator<Account> getAllFirstAccounts();

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number")
  public abstract List<Account> getAllByNumber(@Bind("number") String number);

  @Transaction(TransactionIsolationLevel.SERIALIZABLE)
  public long insertClearingNumber(Account account) {
    removeAccountsByNumber(account.getNumber());
    account.setDeviceId(getHighestDeviceId(account.getNumber()) + 1);
    return insertStep(account);
  }

  public static class AccountMapper implements ResultSetMapper<Account> {

    @Override
    public Account map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      return new Account(resultSet.getLong(ID), resultSet.getString(NUMBER), resultSet.getLong(DEVICE_ID),
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
        return new Binder<AccountBinder, Account>() {
          @Override
          public void bind(SQLStatement<?> sql,
                           AccountBinder accountBinder,
                           Account account)
          {
            sql.bind(ID, account.getId());
            sql.bind(NUMBER, account.getNumber());
            sql.bind(DEVICE_ID, account.getDeviceId());
            sql.bind(AUTH_TOKEN, account.getAuthenticationCredentials()
                                        .getHashedAuthenticationToken());
            sql.bind(SALT, account.getAuthenticationCredentials().getSalt());
            sql.bind(SIGNALING_KEY, account.getSignalingKey());
            sql.bind(GCM_ID, account.getGcmRegistrationId());
            sql.bind(APN_ID, account.getApnRegistrationId());
            sql.bind(SUPPORTS_SMS, account.getSupportsSms() ? 1 : 0);
            sql.bind(FETCHES_MESSAGES, account.getFetchesMessages() ? 1 : 0);
          }
        };
      }
    }
  }

}
