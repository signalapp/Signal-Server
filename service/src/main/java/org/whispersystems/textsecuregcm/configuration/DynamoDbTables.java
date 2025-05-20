/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public class DynamoDbTables {

  public static class Table {
    private final String tableName;

    @JsonCreator
    public Table(
        @JsonProperty("tableName") final String tableName) {
      this.tableName = tableName;
    }

    @NotEmpty
    public String getTableName() {
      return tableName;
    }
  }

  public static class TableWithExpiration extends Table {
    private final Duration expiration;

    @JsonCreator
    public TableWithExpiration(
        @JsonProperty("tableName") final String tableName,
        @JsonProperty("expiration") final Duration expiration) {
      super(tableName);
      this.expiration = expiration;
    }

    @NotNull
    public Duration getExpiration() {
      return expiration;
    }
  }

  private final AccountsTableConfiguration accounts;

  private final Table appleDeviceChecks;
  private final Table appleDeviceCheckPublicKeys;
  private final Table backups;
  private final Table clientPublicKeys;
  private final Table clientReleases;
  private final Table deletedAccounts;
  private final Table deletedAccountsLock;
  private final IssuedReceiptsTableConfiguration issuedReceipts;
  private final Table ecKeys;
  private final Table ecSignedPreKeys;
  private final Table kemKeys;
  private final Table kemLastResortKeys;
  private final Table pagedKemKeys;
  private final TableWithExpiration messages;
  private final TableWithExpiration onetimeDonations;
  private final Table phoneNumberIdentifiers;
  private final Table profiles;
  private final Table pushChallenge;
  private final Table pushNotificationExperimentSamples;
  private final TableWithExpiration redeemedReceipts;
  private final TableWithExpiration registrationRecovery;
  private final Table remoteConfig;
  private final Table reportMessage;
  private final TableWithExpiration scheduledJobs;
  private final Table subscriptions;
  private final Table verificationSessions;

  public DynamoDbTables(
      @JsonProperty("accounts") final AccountsTableConfiguration accounts,
      @JsonProperty("appleDeviceChecks") final Table appleDeviceChecks,
      @JsonProperty("appleDeviceCheckPublicKeys") final Table appleDeviceCheckPublicKeys,
      @JsonProperty("backups") final Table backups,
      @JsonProperty("clientPublicKeys") final Table clientPublicKeys,
      @JsonProperty("clientReleases") final Table clientReleases,
      @JsonProperty("deletedAccounts") final Table deletedAccounts,
      @JsonProperty("deletedAccountsLock") final Table deletedAccountsLock,
      @JsonProperty("issuedReceipts") final IssuedReceiptsTableConfiguration issuedReceipts,
      @JsonProperty("ecKeys") final Table ecKeys,
      @JsonProperty("ecSignedPreKeys") final Table ecSignedPreKeys,
      @JsonProperty("pqKeys") final Table kemKeys,
      @JsonProperty("pqLastResortKeys") final Table kemLastResortKeys,
      @JsonProperty("pagedPqKeys") final Table pagedKemKeys,
      @JsonProperty("messages") final TableWithExpiration messages,
      @JsonProperty("onetimeDonations") final TableWithExpiration onetimeDonations,
      @JsonProperty("phoneNumberIdentifiers") final Table phoneNumberIdentifiers,
      @JsonProperty("profiles") final Table profiles,
      @JsonProperty("pushChallenge") final Table pushChallenge,
      @JsonProperty("pushNotificationExperimentSamples") final Table pushNotificationExperimentSamples,
      @JsonProperty("redeemedReceipts") final TableWithExpiration redeemedReceipts,
      @JsonProperty("registrationRecovery") final TableWithExpiration registrationRecovery,
      @JsonProperty("remoteConfig") final Table remoteConfig,
      @JsonProperty("reportMessage") final Table reportMessage,
      @JsonProperty("scheduledJobs") final TableWithExpiration scheduledJobs,
      @JsonProperty("subscriptions") final Table subscriptions,
      @JsonProperty("verificationSessions") final Table verificationSessions) {

    this.accounts = accounts;
    this.appleDeviceChecks = appleDeviceChecks;
    this.appleDeviceCheckPublicKeys = appleDeviceCheckPublicKeys;
    this.backups = backups;
    this.clientPublicKeys = clientPublicKeys;
    this.clientReleases = clientReleases;
    this.deletedAccounts = deletedAccounts;
    this.deletedAccountsLock = deletedAccountsLock;
    this.issuedReceipts = issuedReceipts;
    this.ecKeys = ecKeys;
    this.ecSignedPreKeys = ecSignedPreKeys;
    this.kemKeys = kemKeys;
    this.pagedKemKeys = pagedKemKeys;
    this.kemLastResortKeys = kemLastResortKeys;
    this.messages = messages;
    this.onetimeDonations = onetimeDonations;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.profiles = profiles;
    this.pushChallenge = pushChallenge;
    this.pushNotificationExperimentSamples = pushNotificationExperimentSamples;
    this.redeemedReceipts = redeemedReceipts;
    this.registrationRecovery = registrationRecovery;
    this.remoteConfig = remoteConfig;
    this.reportMessage = reportMessage;
    this.scheduledJobs = scheduledJobs;
    this.subscriptions = subscriptions;
    this.verificationSessions = verificationSessions;
  }

  @NotNull
  @Valid
  public AccountsTableConfiguration getAccounts() {
    return accounts;
  }

  @NotNull
  @Valid
  public Table getAppleDeviceChecks() {
    return appleDeviceChecks;
  }

  @NotNull
  @Valid
  public Table getAppleDeviceCheckPublicKeys() {
    return appleDeviceCheckPublicKeys;
  }

  @NotNull
  @Valid
  public Table getBackups() {
    return backups;
  }

  @NotNull
  @Valid
  public Table getClientPublicKeys() {
    return clientPublicKeys;
  }

  @NotNull
  @Valid
  public Table getClientReleases() {
    return clientReleases;
  }

  @NotNull
  @Valid
  public Table getDeletedAccounts() {
    return deletedAccounts;
  }

  @NotNull
  @Valid
  public Table getDeletedAccountsLock() {
    return deletedAccountsLock;
  }

  @NotNull
  @Valid
  public IssuedReceiptsTableConfiguration getIssuedReceipts() {
    return issuedReceipts;
  }

  @NotNull
  @Valid
  public Table getEcKeys() {
    return ecKeys;
  }

  @NotNull
  @Valid
  public Table getEcSignedPreKeys() {
    return ecSignedPreKeys;
  }

  @NotNull
  @Valid
  public Table getKemKeys() {
    return kemKeys;
  }

  @NotNull
  @Valid
  public Table getPagedKemKeys() {
    return pagedKemKeys;
  }

  @NotNull
  @Valid
  public Table getKemLastResortKeys() {
    return kemLastResortKeys;
  }

  @NotNull
  @Valid
  public TableWithExpiration getMessages() {
    return messages;
  }

  @NotNull
  @Valid
  public TableWithExpiration getOnetimeDonations() {
    return onetimeDonations;
  }

  @NotNull
  @Valid
  public Table getPhoneNumberIdentifiers() {
    return phoneNumberIdentifiers;
  }

  @NotNull
  @Valid
  public Table getProfiles() {
    return profiles;
  }

  @NotNull
  @Valid
  public Table getPushChallenge() {
    return pushChallenge;
  }

  @NotNull
  @Valid
  public Table getPushNotificationExperimentSamples() {
    return pushNotificationExperimentSamples;
  }

  @NotNull
  @Valid
  public TableWithExpiration getRedeemedReceipts() {
    return redeemedReceipts;
  }

  @NotNull
  @Valid
  public TableWithExpiration getRegistrationRecovery() {
    return registrationRecovery;
  }

  @NotNull
  @Valid
  public Table getRemoteConfig() {
    return remoteConfig;
  }

  @NotNull
  @Valid
  public Table getReportMessage() {
    return reportMessage;
  }

  @NotNull
  @Valid
  public TableWithExpiration getScheduledJobs() {
    return scheduledJobs;
  }

  @NotNull
  @Valid
  public Table getSubscriptions() {
    return subscriptions;
  }

  @NotNull
  @Valid
  public Table getVerificationSessions() {
    return verificationSessions;
  }
}
