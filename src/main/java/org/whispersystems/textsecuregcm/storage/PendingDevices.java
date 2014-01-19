/**
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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface PendingDevices {

  @SqlUpdate("WITH upsert AS (UPDATE pending_devices SET verification_code = :verification_code WHERE number = :number RETURNING *) " +
             "INSERT INTO pending_devices (number, verification_code) SELECT :number, :verification_code WHERE NOT EXISTS (SELECT * FROM upsert)")
  void insert(@Bind("number") String number, @Bind("verification_code") String verificationCode);

  @SqlQuery("SELECT verification_code FROM pending_devices WHERE number = :number")
  String getCodeForNumber(@Bind("number") String number);

  @SqlUpdate("DELETE FROM pending_devices WHERE number = :number")
  void remove(@Bind("number") String number);
}
