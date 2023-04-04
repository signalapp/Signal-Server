package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import javax.validation.constraints.NotEmpty;
import java.util.HexFormat;

public record CallLinkConfiguration (@ExactlySize({32}) byte[] userAuthenticationTokenSharedSecret) {
}
