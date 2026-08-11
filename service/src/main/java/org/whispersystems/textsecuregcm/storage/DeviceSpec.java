package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.util.EncryptDeviceCreationTimestampUtil;
import org.whispersystems.textsecuregcm.util.Util;

public record DeviceSpec(
    byte[] deviceNameCiphertext,
    String password,
    String signalAgent,
    Set<DeviceCapability> capabilities,
    DeviceIdentityInfo aciInfo,
    Optional<DeviceIdentityInfo> pniInfo,
    boolean fetchesMessages,
    Optional<ApnRegistrationId> apnRegistrationId,
    Optional<GcmRegistrationId> gcmRegistrationId) {
  
  public Device toDevice(final byte deviceId, final Clock clock, final IdentityKey aciIdentityKey) {
    final long created = clock.millis();

    final Device device = new Device();
    device.setId(deviceId);
    device.setAuthTokenHash(SaltedTokenHash.generateFor(password()));
    device.setFetchesMessages(fetchesMessages());
    device.setRegistrationId(aciInfo.registrationId());
    pniInfo().ifPresent(pniInfo -> device.setPhoneNumberIdentityRegistrationId(pniInfo.registrationId()));
    device.setName(deviceNameCiphertext());
    device.setCapabilities(capabilities());
    device.setCreated(created);
    device.setCreatedAtCiphertext(
        EncryptDeviceCreationTimestampUtil.encrypt(created, aciIdentityKey, deviceId, aciInfo.registrationId()));
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

    return Objects.equals(aciInfo, that.aciInfo)
        && Objects.equals(pniInfo, that.pniInfo)
        && fetchesMessages == that.fetchesMessages
        && Arrays.equals(deviceNameCiphertext, that.deviceNameCiphertext)
        && Objects.equals(password, that.password)
        && Objects.equals(signalAgent, that.signalAgent)
        && Objects.equals(capabilities, that.capabilities)
        && Objects.equals(apnRegistrationId, that.apnRegistrationId)
        && Objects.equals(gcmRegistrationId, that.gcmRegistrationId);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(password, signalAgent, capabilities, aciInfo, pniInfo,
        fetchesMessages, apnRegistrationId, gcmRegistrationId);
    result = 31 * result + Arrays.hashCode(deviceNameCiphertext);
    return result;
  }
}
