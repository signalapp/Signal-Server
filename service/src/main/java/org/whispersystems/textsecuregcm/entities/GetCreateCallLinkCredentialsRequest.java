package org.whispersystems.textsecuregcm.entities;

import javax.validation.constraints.NotEmpty;


public record GetCreateCallLinkCredentialsRequest(@NotEmpty byte[] createCallLinkCredentialRequest) {}
