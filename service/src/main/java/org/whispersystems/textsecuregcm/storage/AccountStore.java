package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import java.util.UUID;

public interface AccountStore {

  boolean create(Account account);

  void update(Account account);

  Optional<Account> get(String number);

  Optional<Account> get(UUID uuid);

  void delete(final UUID uuid);
}
