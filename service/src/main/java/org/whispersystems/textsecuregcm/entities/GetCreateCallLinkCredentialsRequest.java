package org.whispersystems.textsecuregcm.entities;

import jakarta.validation.constraints.NotEmpty;


public record GetCreateCallLinkCredentialsRequest(@NotEmpty byte[] createCallLinkCredentialRequest) {}
