package org.whispersystems.textsecuregcm.entities;

import javax.validation.constraints.NotNull;

public record GetCreateCallLinkCredentialsRequest(@NotNull byte[] createCallLinkCredentialRequest) {}
