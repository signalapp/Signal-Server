package org.whispersystems.textsecuregcm.storage;

public class PublicAccount extends Account {

  public PublicAccount() {}

  public PublicAccount(Account account) {
    setIdentityKey(account.getIdentityKey());
    setUnidentifiedAccessKey(account.getUnidentifiedAccessKey().orElse(null));
    setUnrestrictedUnidentifiedAccess(account.isUnrestrictedUnidentifiedAccess());
    setAvatar(account.getAvatar());
    setProfileName(account.getProfileName());
    setPin("******");

    account.getDevices().forEach(this::addDevice);
  }

}
