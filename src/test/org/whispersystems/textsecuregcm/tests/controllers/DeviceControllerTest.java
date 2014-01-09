package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.VerificationCode;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeviceControllerTest extends ResourceTest {
  @Path("/v1/devices")
  static class DumbVerificationDeviceController extends DeviceController {
    public DumbVerificationDeviceController(PendingDevicesManager pendingDevices, AccountsManager accounts, RateLimiters rateLimiters) {
      super(pendingDevices, accounts, rateLimiters);
    }

    @Override
    protected VerificationCode generateVerificationCode() {
      return new VerificationCode(5678901);
    }
  }

  private static final String SENDER = "+14152222222";

  private PendingDevicesManager pendingDevicesManager = mock(PendingDevicesManager.class);
  private AccountsManager       accountsManager       = mock(AccountsManager.class       );
  private RateLimiters          rateLimiters          = mock(RateLimiters.class          );
  private RateLimiter           rateLimiter           = mock(RateLimiter.class           );

  @Override
  protected void setUpResources() throws Exception {
    addProvider(AuthHelper.getAuthenticator());

    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);

    when(pendingDevicesManager.getCodeForNumber(AuthHelper.VALID_NUMBER)).thenReturn(Optional.of("5678901"));

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        ((Account) invocation.getArguments()[0]).setDeviceId(2);
        return null;
      }
    }).when(accountsManager).createAccountOnExistingNumber(any(Account.class));

    addResource(new DumbVerificationDeviceController(pendingDevicesManager, accountsManager, rateLimiters));
  }

  @Test
  public void validDeviceRegisterTest() throws Exception {
    VerificationCode deviceCode = client().resource("/v1/devices/")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(VerificationCode.class);

    assertThat(deviceCode).isEqualTo(new VerificationCode(5678901));

    Long deviceId = client().resource(String.format("/v1/devices/5678901"))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, "password1"))
        .entity(new AccountAttributes("keykeykeykey", false, true))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .put(Long.class);
    assertThat(deviceId).isNotEqualTo(AuthHelper.DEFAULT_DEVICE_ID);

    ArgumentCaptor<Account> newAccount = ArgumentCaptor.forClass(Account.class);
    verify(accountsManager).createAccountOnExistingNumber(newAccount.capture());
    assertThat(deviceId).isEqualTo(newAccount.getValue().getDeviceId());

    ArgumentCaptor<String> number = ArgumentCaptor.forClass(String.class);
    verify(pendingDevicesManager).remove(number.capture());
    assertThat(number.getValue()).isEqualTo(AuthHelper.VALID_NUMBER);
  }
}
