/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.ServerPublicParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.groups.ClientZkGroupCipher;
import org.signal.libsignal.zkgroup.groups.GroupMasterKey;
import org.signal.libsignal.zkgroup.groups.GroupSecretParams;
import org.signal.libsignal.zkgroup.groups.UuidCiphertext;
import org.signal.libsignal.zkgroup.groupsend.GroupSendDerivedKeyPair;
import org.signal.libsignal.zkgroup.groupsend.GroupSendEndorsementsResponse;
import org.signal.libsignal.zkgroup.groupsend.GroupSendFullToken;
import org.signal.libsignal.zkgroup.groupsend.GroupSendEndorsementsResponse.ReceivedEndorsements;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

public class AuthHelper {
  // Static seed to ensure reproducible tests.
  private static final Random random = new Random(0xf744df3b43a3339cL);

  public static final TestAccount[] TEST_ACCOUNTS = generateTestAccounts();

  public static final String VALID_NUMBER   = "+14150000000";
  public static final UUID   VALID_UUID     = UUID.randomUUID();
  public static final UUID   VALID_PNI      = UUID.randomUUID();
  public static final String VALID_PASSWORD = "foo";

  public static final String VALID_NUMBER_TWO = "+201511111110";
  public static final UUID   VALID_UUID_TWO    = UUID.randomUUID();
  public static final UUID   VALID_PNI_TWO     = UUID.randomUUID();
  public static final String VALID_PASSWORD_TWO = "baz";

  public static final String VALID_NUMBER_3           = "+14445556666";
  public static final UUID   VALID_UUID_3             = UUID.randomUUID();
  public static final UUID   VALID_PNI_3              = UUID.randomUUID();
  public static final String VALID_PASSWORD_3_PRIMARY = "3primary";
  public static final String VALID_PASSWORD_3_LINKED  = "3linked";

  public static final UUID   INVALID_UUID     = UUID.randomUUID();
  public static final String INVALID_PASSWORD = "bar";

  public static final String UNDISCOVERABLE_NUMBER   = "+18005551234";
  public static final UUID   UNDISCOVERABLE_UUID     = UUID.randomUUID();
  public static final UUID   UNDISCOVERABLE_PNI      = UUID.randomUUID();
  public static final String UNDISCOVERABLE_PASSWORD = "IT'S A SECRET TO EVERYBODY.";

  public static final ECKeyPair VALID_IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  public static final IdentityKey VALID_IDENTITY = new IdentityKey(VALID_IDENTITY_KEY_PAIR.getPublicKey());

  public static final ECKeyPair VALID_PNI_IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  public static final IdentityKey VALID_PNI_IDENTITY = new IdentityKey(VALID_PNI_IDENTITY_KEY_PAIR.getPublicKey());

  public static AccountsManager ACCOUNTS_MANAGER       = mock(AccountsManager.class);
  public static Account         VALID_ACCOUNT          = mock(Account.class        );
  public static Account         VALID_ACCOUNT_TWO      = mock(Account.class        );
  public static Account         UNDISCOVERABLE_ACCOUNT = mock(Account.class        );
  public static Account         VALID_ACCOUNT_3        = mock(Account.class        );

  public static Device VALID_DEVICE           = mock(Device.class);
  public static Device VALID_DEVICE_TWO       = mock(Device.class);
  public static Device UNDISCOVERABLE_DEVICE  = mock(Device.class);
  public static Device VALID_DEVICE_3_PRIMARY = mock(Device.class);
  public static Device VALID_DEVICE_3_LINKED  = mock(Device.class);

  public static final byte VALID_DEVICE_3_LINKED_ID = Device.PRIMARY_ID + 1;

  private static SaltedTokenHash VALID_CREDENTIALS           = mock(SaltedTokenHash.class);
  private static SaltedTokenHash VALID_CREDENTIALS_TWO       = mock(SaltedTokenHash.class);
  private static SaltedTokenHash VALID_CREDENTIALS_3_PRIMARY = mock(SaltedTokenHash.class);
  private static SaltedTokenHash VALID_CREDENTIALS_3_LINKED  = mock(SaltedTokenHash.class);
  private static SaltedTokenHash UNDISCOVERABLE_CREDENTIALS  = mock(SaltedTokenHash.class);

  private static final Collection<TestAccount> EXTENSION_TEST_ACCOUNTS = new HashSet<>();

  public static PolymorphicAuthDynamicFeature<? extends Principal> getAuthFilter() {
    when(VALID_CREDENTIALS.verify("foo")).thenReturn(true);
    when(VALID_CREDENTIALS_TWO.verify("baz")).thenReturn(true);
    when(VALID_CREDENTIALS_3_PRIMARY.verify(VALID_PASSWORD_3_PRIMARY)).thenReturn(true);
    when(VALID_CREDENTIALS_3_LINKED.verify(VALID_PASSWORD_3_LINKED)).thenReturn(true);
    when(UNDISCOVERABLE_CREDENTIALS.verify(UNDISCOVERABLE_PASSWORD)).thenReturn(true);

    when(VALID_DEVICE.getAuthTokenHash()).thenReturn(VALID_CREDENTIALS);
    when(VALID_DEVICE_TWO.getAuthTokenHash()).thenReturn(VALID_CREDENTIALS_TWO);
    when(VALID_DEVICE_3_PRIMARY.getAuthTokenHash()).thenReturn(VALID_CREDENTIALS_3_PRIMARY);
    when(VALID_DEVICE_3_LINKED.getAuthTokenHash()).thenReturn(VALID_CREDENTIALS_3_LINKED);
    when(UNDISCOVERABLE_DEVICE.getAuthTokenHash()).thenReturn(UNDISCOVERABLE_CREDENTIALS);

    when(VALID_DEVICE.isPrimary()).thenReturn(true);
    when(VALID_DEVICE_TWO.isPrimary()).thenReturn(true);
    when(UNDISCOVERABLE_DEVICE.isPrimary()).thenReturn(true);
    when(VALID_DEVICE_3_PRIMARY.isPrimary()).thenReturn(true);
    when(VALID_DEVICE_3_LINKED.isPrimary()).thenReturn(false);

    when(VALID_DEVICE.getId()).thenReturn(Device.PRIMARY_ID);
    when(VALID_DEVICE_TWO.getId()).thenReturn(Device.PRIMARY_ID);
    when(UNDISCOVERABLE_DEVICE.getId()).thenReturn(Device.PRIMARY_ID);
    when(VALID_DEVICE_3_PRIMARY.getId()).thenReturn(Device.PRIMARY_ID);
    when(VALID_DEVICE_3_LINKED.getId()).thenReturn(VALID_DEVICE_3_LINKED_ID);

    when(UNDISCOVERABLE_DEVICE.isPrimary()).thenReturn(true);

    when(VALID_ACCOUNT.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(VALID_DEVICE));
    when(VALID_ACCOUNT.getPrimaryDevice()).thenReturn(VALID_DEVICE);
    when(VALID_ACCOUNT_TWO.getDevice(eq(Device.PRIMARY_ID))).thenReturn(Optional.of(VALID_DEVICE_TWO));
    when(VALID_ACCOUNT_TWO.getPrimaryDevice()).thenReturn(VALID_DEVICE_TWO);
    when(UNDISCOVERABLE_ACCOUNT.getDevice(eq(Device.PRIMARY_ID))).thenReturn(Optional.of(UNDISCOVERABLE_DEVICE));
    when(UNDISCOVERABLE_ACCOUNT.getPrimaryDevice()).thenReturn(UNDISCOVERABLE_DEVICE);
    when(VALID_ACCOUNT_3.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(VALID_DEVICE_3_PRIMARY));
    when(VALID_ACCOUNT_3.getPrimaryDevice()).thenReturn(VALID_DEVICE_3_PRIMARY);
    when(VALID_ACCOUNT_3.getDevice((byte) 2)).thenReturn(Optional.of(VALID_DEVICE_3_LINKED));

    when(VALID_ACCOUNT.getDevices()).thenReturn(List.of(VALID_DEVICE));
    when(VALID_ACCOUNT_TWO.getDevices()).thenReturn(List.of(VALID_DEVICE_TWO));
    when(UNDISCOVERABLE_ACCOUNT.getDevices()).thenReturn(List.of(UNDISCOVERABLE_DEVICE));
    when(VALID_ACCOUNT_3.getDevices()).thenReturn(List.of(VALID_DEVICE_3_PRIMARY, VALID_DEVICE_3_LINKED));

    when(VALID_ACCOUNT.getNumber()).thenReturn(VALID_NUMBER);
    when(VALID_ACCOUNT.getUuid()).thenReturn(VALID_UUID);
    when(VALID_ACCOUNT.getPhoneNumberIdentifier()).thenReturn(VALID_PNI);
    when(VALID_ACCOUNT.getIdentifier(IdentityType.ACI)).thenReturn(VALID_UUID);
    when(VALID_ACCOUNT.getIdentifier(IdentityType.PNI)).thenReturn(VALID_PNI);
    when(VALID_ACCOUNT_TWO.getNumber()).thenReturn(VALID_NUMBER_TWO);
    when(VALID_ACCOUNT_TWO.getUuid()).thenReturn(VALID_UUID_TWO);
    when(VALID_ACCOUNT_TWO.getPhoneNumberIdentifier()).thenReturn(VALID_PNI_TWO);
    when(VALID_ACCOUNT_TWO.getIdentifier(IdentityType.ACI)).thenReturn(VALID_UUID_TWO);
    when(VALID_ACCOUNT_TWO.getPhoneNumberIdentifier()).thenReturn(VALID_PNI_TWO);
    when(UNDISCOVERABLE_ACCOUNT.getNumber()).thenReturn(UNDISCOVERABLE_NUMBER);
    when(UNDISCOVERABLE_ACCOUNT.getUuid()).thenReturn(UNDISCOVERABLE_UUID);
    when(UNDISCOVERABLE_ACCOUNT.getPhoneNumberIdentifier()).thenReturn(UNDISCOVERABLE_PNI);
    when(UNDISCOVERABLE_ACCOUNT.getIdentifier(IdentityType.ACI)).thenReturn(UNDISCOVERABLE_UUID);
    when(UNDISCOVERABLE_ACCOUNT.getIdentifier(IdentityType.PNI)).thenReturn(UNDISCOVERABLE_PNI);
    when(VALID_ACCOUNT_3.getNumber()).thenReturn(VALID_NUMBER_3);
    when(VALID_ACCOUNT_3.getUuid()).thenReturn(VALID_UUID_3);
    when(VALID_ACCOUNT_3.getPhoneNumberIdentifier()).thenReturn(VALID_PNI_3);
    when(VALID_ACCOUNT_3.getIdentifier(IdentityType.ACI)).thenReturn(VALID_UUID_3);
    when(VALID_ACCOUNT_3.getIdentifier(IdentityType.PNI)).thenReturn(VALID_PNI_3);

    when(VALID_ACCOUNT.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(VALID_ACCOUNT_TWO.isDiscoverableByPhoneNumber()).thenReturn(true);
    when(UNDISCOVERABLE_ACCOUNT.isDiscoverableByPhoneNumber()).thenReturn(false);
    when(VALID_ACCOUNT_3.isDiscoverableByPhoneNumber()).thenReturn(true);

    when(VALID_ACCOUNT.isIdentifiedBy(new AciServiceIdentifier(VALID_UUID))).thenReturn(true);
    when(VALID_ACCOUNT.isIdentifiedBy(new PniServiceIdentifier(VALID_PNI))).thenReturn(true);
    when(VALID_ACCOUNT_TWO.isIdentifiedBy(new AciServiceIdentifier(VALID_UUID_TWO))).thenReturn(true);
    when(VALID_ACCOUNT_TWO.isIdentifiedBy(new PniServiceIdentifier(VALID_PNI_TWO))).thenReturn(true);
    when(UNDISCOVERABLE_ACCOUNT.isIdentifiedBy(new AciServiceIdentifier(UNDISCOVERABLE_UUID))).thenReturn(true);
    when(VALID_ACCOUNT_3.isIdentifiedBy(new AciServiceIdentifier(VALID_UUID_3))).thenReturn(true);
    when(VALID_ACCOUNT_3.isIdentifiedBy(new PniServiceIdentifier(VALID_PNI_3))).thenReturn(true);

    when(VALID_ACCOUNT.getIdentityKey(IdentityType.ACI)).thenReturn(VALID_IDENTITY);
    when(VALID_ACCOUNT.getIdentityKey(IdentityType.PNI)).thenReturn(VALID_PNI_IDENTITY);

    reset(ACCOUNTS_MANAGER);

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER)).thenReturn(Optional.of(VALID_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID)).thenReturn(Optional.of(VALID_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI)).thenReturn(Optional.of(VALID_ACCOUNT));

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI_TWO)).thenReturn(Optional.of(VALID_ACCOUNT_TWO));

    when(ACCOUNTS_MANAGER.getByE164(UNDISCOVERABLE_NUMBER)).thenReturn(Optional.of(UNDISCOVERABLE_ACCOUNT));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(UNDISCOVERABLE_UUID)).thenReturn(Optional.of(UNDISCOVERABLE_ACCOUNT));

    when(ACCOUNTS_MANAGER.getByE164(VALID_NUMBER_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));
    when(ACCOUNTS_MANAGER.getByAccountIdentifier(VALID_UUID_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));
    when(ACCOUNTS_MANAGER.getByPhoneNumberIdentifier(VALID_PNI_3)).thenReturn(Optional.of(VALID_ACCOUNT_3));

    AccountsHelper.setupMockUpdateForAuthHelper(ACCOUNTS_MANAGER);

    for (TestAccount testAccount : TEST_ACCOUNTS) {
      testAccount.setup(ACCOUNTS_MANAGER);
    }

    AuthFilter<BasicCredentials, AuthenticatedDevice> accountAuthFilter = new BasicCredentialAuthFilter.Builder<AuthenticatedDevice>().setAuthenticator(
        new AccountAuthenticator(ACCOUNTS_MANAGER)).buildAuthFilter();

    return new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(AuthenticatedDevice.class, accountAuthFilter));
  }

  public static String getAuthHeader(UUID uuid, byte deviceId, String password) {
    return HeaderUtils.basicAuthHeader(uuid.toString() + "." + deviceId, password);
  }

  public static String getAuthHeader(UUID uuid, String password) {
    return HeaderUtils.basicAuthHeader(uuid.toString(), password);
  }

  public static String getProvisioningAuthHeader(String number, String password) {
    return HeaderUtils.basicAuthHeader(number, password);
  }

  public static String getUnidentifiedAccessHeader(byte[] key) {
    return Base64.getEncoder().encodeToString(key);
  }

  public static UUID getRandomUUID(Random random) {
    long mostSignificantBits  = random.nextLong();
    long leastSignificantBits = random.nextLong();
    mostSignificantBits  &= 0xffffffffffff0fffL;
    mostSignificantBits  |= 0x0000000000004000L;
    leastSignificantBits &= 0x3fffffffffffffffL;
    leastSignificantBits |= 0x8000000000000000L;
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  public static final class TestAccount {
    public final String                    number;
    public final UUID                      uuid;
    public final String                    password;
    public final Account                   account                   = mock(Account.class);
    public final Device                    device                    = mock(Device.class);
    public final SaltedTokenHash saltedTokenHash = mock(SaltedTokenHash.class);

    public TestAccount(String number, UUID uuid, String password) {
      this.number = number;
      this.uuid = uuid;
      this.password = password;
    }

    public String getAuthHeader() {
      return AuthHelper.getAuthHeader(uuid, password);
    }

    private void setup(final AccountsManager accountsManager) {
      when(saltedTokenHash.verify(password)).thenReturn(true);
      when(device.getAuthTokenHash()).thenReturn(saltedTokenHash);
      when(device.isPrimary()).thenReturn(true);
      when(device.getId()).thenReturn(Device.PRIMARY_ID);
      when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(device));
      when(account.getPrimaryDevice()).thenReturn(device);
      when(account.getNumber()).thenReturn(number);
      when(account.getUuid()).thenReturn(uuid);
      when(account.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
      when(accountsManager.getByE164(number)).thenReturn(Optional.of(account));
      when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
    }

    private void teardown(final AccountsManager accountsManager) {
      when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.empty());
      when(accountsManager.getByE164(number)).thenReturn(Optional.empty());
    }
  }

  private static TestAccount[] generateTestAccounts() {
    final TestAccount[] testAccounts = new TestAccount[20];
    final long numberBase = 1_409_000_0000L;
    for (int i = 0; i < testAccounts.length; i++) {
      long currentNumber = numberBase + i;
      testAccounts[i] = new TestAccount("+" + currentNumber, getRandomUUID(random), "TestAccountPassword-" + currentNumber);
    }
    return testAccounts;
  }

  /**
   * JUnit 5 extension for creating {@link TestAccount}s scoped to a single test
   */
  public static class AuthFilterExtension implements AfterEachCallback {

    public TestAccount createTestAccount() {
      final UUID uuid = UUID.randomUUID();
      final String region = new ArrayList<>((PhoneNumberUtil.getInstance().getSupportedRegions())).get(
          EXTENSION_TEST_ACCOUNTS.size());
      final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance().getExampleNumber(region);

      final TestAccount testAccount = new TestAccount(
          PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164), uuid,
          "extension-password-" + region);
      testAccount.setup(ACCOUNTS_MANAGER);

      EXTENSION_TEST_ACCOUNTS.add(testAccount);

      return testAccount;
    }

    @Override
    public void afterEach(final ExtensionContext context) {
      EXTENSION_TEST_ACCOUNTS.forEach(testAccount -> testAccount.teardown(ACCOUNTS_MANAGER));

      EXTENSION_TEST_ACCOUNTS.clear();
    }
  }

  public static byte[] validGroupSendToken(ServerSecretParams serverSecretParams, List<ServiceIdentifier> recipients, Instant expiration) throws Exception {
    final ServerPublicParams serverPublicParams = serverSecretParams.getPublicParams();
    final GroupMasterKey groupMasterKey = new GroupMasterKey(new byte[32]);
    final GroupSecretParams groupSecretParams = GroupSecretParams.deriveFromMasterKey(groupMasterKey);
    final ClientZkGroupCipher clientZkGroupCipher = new ClientZkGroupCipher(groupSecretParams);

    final ServiceId.Aci sender = new ServiceId.Aci(UUID.randomUUID());
    List<ServiceId> groupPlaintexts = Stream.concat(Stream.of(sender), recipients.stream().map(ServiceIdentifier::toLibsignal)).toList();
    List<UuidCiphertext> groupCiphertexts = groupPlaintexts.stream()
        .map(clientZkGroupCipher::encrypt)
        .toList();
    GroupSendDerivedKeyPair keyPair = GroupSendDerivedKeyPair.forExpiration(expiration, serverSecretParams);
    GroupSendEndorsementsResponse endorsementsResponse =
        GroupSendEndorsementsResponse.issue(groupCiphertexts, keyPair);
    ReceivedEndorsements endorsements =
        endorsementsResponse.receive(
            groupPlaintexts,
            sender,
            expiration.minus(Duration.ofDays(1)),
            groupSecretParams,
            serverPublicParams);
    GroupSendFullToken token = endorsements.combinedEndorsement().toFullToken(groupSecretParams, expiration);
    return token.serialize();
  }

  public static String validGroupSendTokenHeader(ServerSecretParams serverSecretParams, List<ServiceIdentifier> recipients, Instant expiration) throws Exception {
    return Base64.getEncoder().encodeToString(validGroupSendToken(serverSecretParams, recipients, expiration));
  }
}
