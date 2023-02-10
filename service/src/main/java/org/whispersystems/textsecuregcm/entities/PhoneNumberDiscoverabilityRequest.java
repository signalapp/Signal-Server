package org.whispersystems.textsecuregcm.entities;

import javax.validation.constraints.NotNull;

public record PhoneNumberDiscoverabilityRequest(@NotNull Boolean discoverableByPhoneNumber) {}
