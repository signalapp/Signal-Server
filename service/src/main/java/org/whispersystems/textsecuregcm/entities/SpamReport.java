package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;

public record SpamReport(@JsonProperty("token") @NotEmpty String token) {}
