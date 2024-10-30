package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.util.Util;

public record DeviceSpec(
    byte[] deviceNameCiphertext,
    String password,
    String signalAgent,
    Set<DeviceCapability> capabilities,
    int aciRegistrationId,
    int pniRegistrationId,
    boolean fetchesMessages,
    Optional<ApnRegistrationId> apnRegistrationId,
    Optional<GcmRegistrationId> gcmRegistrationId,
    ECSignedPreKey aciSignedPreKey,
    ECSignedPreKey pniSignedPreKey,
    KEMSignedPreKey aciPqLastResortPreKey,
    KEMSignedPreKey pniPqLastResortPreKey) {
  
  public Device toDevice(final byte deviceId, final Clock clock) {
    final Device device = new Device();
    device.setId(deviceId);
    device.setAuthTokenHash(SaltedTokenHash.generateFor(password()));
    device.setFetchesMessages(fetchesMessages());
    device.setRegistrationId(aciRegistrationId());
    device.setPhoneNumberIdentityRegistrationId(pniRegistrationId());
    device.setName(deviceNameCiphertext());
    device.setCapabilities(capabilities());
    device.setCreated(clock.millis());
    device.setLastSeen(Util.todayInMillis());
    device.setUserAgent(signalAgent());

    apnRegistrationId().ifPresent(apnRegistrationId -> device.setApnId(apnRegistrationId.apnRegistrationId()));
    gcmRegistrationId().ifPresent(gcmRegistrationId -> device.setGcmId(gcmRegistrationId.gcmRegistrationId()));

    return device;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DeviceSpec that = (DeviceSpec) o;

    return aciRegistrationId == that.aciRegistrationId
        && pniRegistrationId == that.pniRegistrationId
        && fetchesMessages == that.fetchesMessages
        && Arrays.equals(deviceNameCiphertext, that.deviceNameCiphertext)
        && Objects.equals(password, that.password)
        && Objects.equals(signalAgent, that.signalAgent)
        && Objects.equals(capabilities, that.capabilities)
        && Objects.equals(apnRegistrationId, that.apnRegistrationId)
        && Objects.equals(gcmRegistrationId, that.gcmRegistrationId)
        && Objects.equals(aciSignedPreKey, that.aciSignedPreKey)
        && Objects.equals(pniSignedPreKey, that.pniSignedPreKey)
        && Objects.equals(aciPqLastResortPreKey, that.aciPqLastResortPreKey)
        && Objects.equals(pniPqLastResortPreKey, that.pniPqLastResortPreKey);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(password, signalAgent, capabilities, aciRegistrationId, pniRegistrationId,
        fetchesMessages, apnRegistrationId, gcmRegistrationId, aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey,
        pniPqLastResortPreKey);
    result = 31 * result + Arrays.hashCode(deviceNameCiphertext);
    return result;
  }
}
