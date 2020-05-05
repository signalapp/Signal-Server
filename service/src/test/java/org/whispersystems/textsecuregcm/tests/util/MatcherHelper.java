package org.whispersystems.textsecuregcm.tests.util;

import org.mockito.ArgumentMatchers;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class MatcherHelper {
    public static Account accountWithNumber(final String accountNumber) {
        return ArgumentMatchers.argThat(account -> accountNumber.equals(account.getNumber()));
    }

    public static Device deviceWithId(final long deviceId) {
        return ArgumentMatchers.argThat(device -> device.getId() == deviceId);
    }
}
