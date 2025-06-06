package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;

public record LinkDeviceRequest(@Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
                                The verification code associated with this device. Must match the verification code
                                provided by the server when provisioning this device.
                                """)
                                @NotBlank
                                String verificationCode,

                                @Valid
                                AccountAttributes accountAttributes,

                                @NotNull
                                @Valid
                                @JsonUnwrapped
                                @JsonProperty(access = JsonProperty.Access.READ_ONLY)
                                DeviceActivationRequest deviceActivationRequest) {

  @JsonCreator
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public LinkDeviceRequest(@JsonProperty("verificationCode") String verificationCode,
                           @JsonProperty("accountAttributes") AccountAttributes accountAttributes,
                           @JsonProperty("aciSignedPreKey") @NotNull @Valid ECSignedPreKey aciSignedPreKey,
                           @JsonProperty("pniSignedPreKey") @NotNull @Valid ECSignedPreKey pniSignedPreKey,
                           @JsonProperty("aciPqLastResortPreKey") @NotNull @Valid KEMSignedPreKey aciPqLastResortPreKey,
                           @JsonProperty("pniPqLastResortPreKey") @NotNull @Valid KEMSignedPreKey pniPqLastResortPreKey,
                           @JsonProperty("apnToken") Optional<@Valid ApnRegistrationId> apnToken,
                           @JsonProperty("gcmToken") Optional<@Valid GcmRegistrationId> gcmToken) {

    this(verificationCode, accountAttributes,
        new DeviceActivationRequest(aciSignedPreKey, pniSignedPreKey, aciPqLastResortPreKey, pniPqLastResortPreKey, apnToken, gcmToken));
  }

  @AssertTrue
  @Schema(hidden = true)
  public boolean hasExactlyOneMessageDeliveryChannel() {
    if (accountAttributes.getFetchesMessages()) {
      return deviceActivationRequest().apnToken().isEmpty() && deviceActivationRequest().gcmToken().isEmpty();
    } else {
      return deviceActivationRequest().apnToken().isPresent() ^ deviceActivationRequest().gcmToken().isPresent();
    }
  }
}
