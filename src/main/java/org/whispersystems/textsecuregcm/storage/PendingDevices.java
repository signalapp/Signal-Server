/*
 * Copyright (C) 2014 Open WhisperSystems
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

import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.storage.mappers.StoredVerificationCodeRowMapper;

import java.util.Optional;

public class PendingDevices {

  private final Jdbi database;

  public PendingDevices(Jdbi database) {
    this.database = database;
    this.database.registerRowMapper(new StoredVerificationCodeRowMapper());
  }

  public void insert(String number, String verificationCode, long timestamp) {
    database.useHandle(handle -> handle.createUpdate("WITH upsert AS (UPDATE pending_devices SET verification_code = :verification_code, timestamp = :timestamp WHERE number = :number RETURNING *) " +
                                                         "INSERT INTO pending_devices (number, verification_code, timestamp) SELECT :number, :verification_code, :timestamp WHERE NOT EXISTS (SELECT * FROM upsert)")
                                       .bind("number", number)
                                       .bind("verification_code", verificationCode)
                                       .bind("timestamp", timestamp)
                                       .execute());
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    return database.withHandle(handle -> handle.createQuery("SELECT verification_code, timestamp FROM pending_devices WHERE number = :number")
                                               .bind("number", number)
                                               .mapTo(StoredVerificationCode.class)
                                               .findFirst());
  }

  public void remove(String number) {
    database.useHandle(handle -> handle.createUpdate("DELETE FROM pending_devices WHERE number = :number")
                                       .bind("number", number)
                                       .execute());
  }

}
