/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomUtils;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class SecureBackupControllerTest extends SecureValueRecoveryControllerBaseTest {

  private static final byte[] SECRET = RandomUtils.nextBytes(32);

  private static final SecureBackupServiceConfiguration CFG = MockUtils.buildMock(
      SecureBackupServiceConfiguration.class,
      cfg -> Mockito.when(cfg.getUserAuthenticationTokenSharedSecret()).thenReturn(SECRET)
  );

  private static final MutableClock CLOCK = new MutableClock();

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureBackupController.credentialsGenerator(CFG, CLOCK);


  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);
  private static final SecureBackupController CONTROLLER =
      new SecureBackupController(CREDENTIAL_GENERATOR, ACCOUNTS_MANAGER);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  protected SecureBackupControllerTest() {
    super("/v1", ACCOUNTS_MANAGER, CLOCK, RESOURCES, CREDENTIAL_GENERATOR);
  }
}
