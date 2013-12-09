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

  public static final String ID            = "id";
  public static final String NUMBER        = "number";
  public static final String AUTH_TOKEN    = "auth_token";
  public static final String SALT          = "salt";
  public static final String SIGNALING_KEY = "signaling_key";
  public static final String GCM_ID        = "gcm_id";
  public static final String APN_ID        = "apn_id";
  public static final String SUPPORTS_SMS  = "supports_sms";

  @SqlUpdate("INSERT INTO accounts (" + NUMBER + ", " + AUTH_TOKEN + ", "  +
                                   SALT + ", " + SIGNALING_KEY + ", " + GCM_ID + ", " +
                                   APN_ID + ", " + SUPPORTS_SMS + ") " +
             "VALUES (:number, :auth_token, :salt, :signaling_key, :gcm_id, :apn_id, :supports_sms)")
  @GetGeneratedKeys
  abstract long createStep(@AccountBinder Account account);

  @SqlUpdate("DELETE FROM accounts WHERE number = :number")
  abstract void removeStep(@Bind("number") String number);

  @SqlUpdate("UPDATE accounts SET " + AUTH_TOKEN + " = :auth_token, " + SALT + " = :salt, " +
             SIGNALING_KEY + " = :signaling_key, " + GCM_ID + " = :gcm_id, " +
             APN_ID + " = :apn_id, " + SUPPORTS_SMS + " = :supports_sms " +
             "WHERE " + NUMBER + " = :number")
  abstract void update(@AccountBinder Account account);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number")
  abstract Account get(@Bind("number") String number);

  @SqlQuery("SELECT COUNT(*) from accounts")
  abstract long getCount();

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts OFFSET :offset LIMIT :limit")
  abstract List<Account> getAll(@Bind("offset") int offset, @Bind("limit") int length);

  @Mapper(AccountMapper.class)
  @SqlQuery("SELECT * FROM accounts")
  abstract Iterator<Account> getAll();

  @Transaction(TransactionIsolationLevel.REPEATABLE_READ)
  public long create(Account account) {
    removeStep(account.getNumber());
    return createStep(account);
  }

  public static class AccountMapper implements ResultSetMapper<Account> {

    @Override
    public Account map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      return new Account(resultSet.getLong(ID), resultSet.getString(NUMBER),
                         resultSet.getString(AUTH_TOKEN), resultSet.getString(SALT),
                         resultSet.getString(SIGNALING_KEY), resultSet.getString(GCM_ID),
                         resultSet.getString(APN_ID),
                         resultSet.getInt(SUPPORTS_SMS) == 1);
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
            sql.bind(AUTH_TOKEN, account.getAuthenticationCredentials()
                                        .getHashedAuthenticationToken());
            sql.bind(SALT, account.getAuthenticationCredentials().getSalt());
            sql.bind(SIGNALING_KEY, account.getSignalingKey());
            sql.bind(GCM_ID, account.getGcmRegistrationId());
            sql.bind(APN_ID, account.getApnRegistrationId());
            sql.bind(SUPPORTS_SMS, account.getSupportsSms() ? 1 : 0);
          }
        };
      }
    }
  }

}
