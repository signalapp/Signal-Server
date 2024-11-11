package org.whispersystems.textsecuregcm.entities;

import jakarta.validation.constraints.NotNull;

public record PhoneNumberDiscoverabilityRequest(@NotNull Boolean discoverableByPhoneNumber) {}
