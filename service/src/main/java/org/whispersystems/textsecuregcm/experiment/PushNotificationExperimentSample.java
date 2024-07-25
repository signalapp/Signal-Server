package org.whispersystems.textsecuregcm.experiment;

public record PushNotificationExperimentSample<T>(boolean inExperimentGroup, T initialState, T finalState) {
}
