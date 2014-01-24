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
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

import java.util.List;

public interface StoredMessages {

  @SqlUpdate("INSERT INTO messages (account_id, device_id, encrypted_message) VALUES (:account_id, :device_id, :encrypted_message)")
  void insert(@Bind("account_id") long accountId, @Bind("device_id") long deviceId, @Bind("encrypted_message") String encryptedOutgoingMessage);

  @SqlBatch("INSERT INTO messages (account_id, device_id, encrypted_message) VALUES (:account_id, :device_id, :encrypted_message)")
  void insert(@Bind("account_id") long accountId, @Bind("device_id") long deviceId, @Bind("encrypted_message") List<String> encryptedOutgoingMessages);

  @SqlQuery("DELETE FROM messages WHERE account_id = :account_id AND device_id = :device_id RETURNING encrypted_message")
  List<String> getMessagesForDevice(@Bind("account_id") long accountId, @Bind("device_id") long deviceId);
}
