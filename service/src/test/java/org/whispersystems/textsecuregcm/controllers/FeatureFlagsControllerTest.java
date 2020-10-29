/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.mappers.DeviceLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.FeatureFlagsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class FeatureFlagsControllerTest {

    private static final FeatureFlagsManager    FEATURE_FLAG_MANAGER    = mock(FeatureFlagsManager.class);
    private static final FeatureFlagsController FEATURE_FLAG_CONTROLLER = new FeatureFlagsController(FEATURE_FLAG_MANAGER, List.of("first", "second"));

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addProvider(AuthHelper.getAuthFilter())
            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
            .addProvider(new DeviceLimitExceededExceptionMapper())
            .addResource(FEATURE_FLAG_CONTROLLER)
            .build();

    @Before
    public void setUp() {
        reset(FEATURE_FLAG_MANAGER);
    }

    @Test
    public void testSet() {
        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag/testFlag")
                    .request()
                    .header("Token", "first")
                    .put(Entity.form(new Form().param("active", "true")));

            assertEquals(204, response.getStatus());
            verify(FEATURE_FLAG_MANAGER).setFeatureFlag("testFlag", true);
        }

        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag/secondFlag")
                    .request()
                    .header("Token", "first")
                    .put(Entity.form(new Form().param("active", "false")));

            assertEquals(204, response.getStatus());
            verify(FEATURE_FLAG_MANAGER).setFeatureFlag("secondFlag", false);
        }

        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag/testFlag")
                    .request()
                    .header("Token", "bogus-token")
                    .put(Entity.form(new Form().param("active", "true")));

            assertEquals(401, response.getStatus());
            verifyNoMoreInteractions(FEATURE_FLAG_MANAGER);
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testGet() {
        final Map<String, Boolean> managedFlags = Map.of("activeFlag", true, "inactiveFlag", false);
        when(FEATURE_FLAG_MANAGER.getAllFlags()).thenReturn(managedFlags);

        {
            final Map returnedFlags = resources.getJerseyTest()
                    .target("/v1/featureflag")
                    .request()
                    .header("Token", "first")
                    .get(Map.class);

            verify(FEATURE_FLAG_MANAGER).getAllFlags();
            assertEquals(managedFlags, returnedFlags);
        }

        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag")
                    .request()
                    .header("Token", "bogus-token")
                    .get();

            assertEquals(401, response.getStatus());
            verifyNoMoreInteractions(FEATURE_FLAG_MANAGER);
        }
    }

    @Test
    public void testDelete() {
        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag/testFlag")
                    .request()
                    .header("Token", "first")
                    .delete();

            assertEquals(204, response.getStatus());
            verify(FEATURE_FLAG_MANAGER).deleteFeatureFlag("testFlag");
        }

        {
            final Response response = resources.getJerseyTest()
                    .target("/v1/featureflag/testFlag")
                    .request()
                    .header("Token", "bogus-token")
                    .delete();

            assertEquals(401, response.getStatus());
            verifyNoMoreInteractions(FEATURE_FLAG_MANAGER);
        }
    }

    @Test
    @Parameters(method = "argumentsForTestIsAuthorized")
    public void testIsAuthorized(final String token, final boolean expectAuthorized) {
        assertEquals(expectAuthorized, FEATURE_FLAG_CONTROLLER.isAuthorized(token));
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestIsAuthorized() {
        return new Object[] {
                new Object[] { "first",           true },
                new Object[] { "second",          true },
                new Object[] { "third",           false },
                new Object[] { "firstfirstfirst", false },
                new Object[] { null,              false }
        };
    }
}
