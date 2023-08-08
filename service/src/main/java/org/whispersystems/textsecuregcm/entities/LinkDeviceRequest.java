package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import java.util.Optional;

public record LinkDeviceRequest(@Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
                                The verification code associated with this device. Must match the verification code
                                provided by the server when provisioning this device.
                                """)
                                String verificationCode,

                                AccountAttributes accountAttributes,

                                @JsonUnwrapped
                                @JsonProperty(access = JsonProperty.Access.READ_ONLY)
                                DeviceActivationRequest deviceActivationRequest) {

  @JsonCreator
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public LinkDeviceRequest(@JsonProperty("verificationCode") @NotBlank String verificationCode,
                           @JsonProperty("accountAttributes") @Valid AccountAttributes accountAttributes,
                           @JsonProperty("aciSignedPreKey") Optional<@Valid ECSignedPreKey> aciSignedPreKey,
                           @JsonProperty("pniSignedPreKey") Optional<@Valid ECSignedPreKey> pniSignedPreKey,
                           @JsonProperty("aciPqLastResortPreKey") Optional<@Valid KEMSignedPreKey> aciPqLastResortPreKey,
                           @JsonProperty("pniPqLastResortPreKey") Optional<@Valid KEMSignedPreKey> pniPqLastResortPreKey,
                           @JsonProperty("apnToken") Optional<@Valid ApnRegistrationId> apnToken,
                           @JsonProperty("gcmToken") Optional<@Valid GcmRegistrationId> gcmToken) {

    this(verificationCode, accountAttributes,
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, apnToken, gcmToken));
  }

  @AssertTrue
  public boolean hasAllRequiredFields() {
    // PNI-associated credentials are not yet required, but will be when all devices are assumed to have a PNI identity
    // key.
    final boolean mismatchedPniKeys = deviceActivationRequest().pniSignedPreKey().isPresent()
        ^ deviceActivationRequest().pniPqLastResortPreKey().isPresent();

    return deviceActivationRequest().aciSignedPreKey().isPresent()
        && deviceActivationRequest().aciPqLastResortPreKey().isPresent()
        && !mismatchedPniKeys;
  }

  @AssertTrue
  public boolean hasExactlyOneMessageDeliveryChannel() {
    if (accountAttributes.getFetchesMessages()) {
      return deviceActivationRequest().apnToken().isEmpty() && deviceActivationRequest().gcmToken().isEmpty();
    } else {
      return deviceActivationRequest().apnToken().isPresent() ^ deviceActivationRequest().gcmToken().isPresent();
    }
  }
}
