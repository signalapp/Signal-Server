package org.whispersystems.textsecuregcm.experiment;

import javax.annotation.Nullable;
import java.util.UUID;

public record PushNotificationExperimentSample<T>(UUID accountIdentifier,
                                                  byte deviceId,
                                                  boolean inExperimentGroup,
                                                  T initialState,
                                                  @Nullable T finalState) {
}
