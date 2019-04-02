package org.whispersystems.textsecuregcm.tests.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.mappers.AccountRowMapper;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AccountsTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private Accounts accounts;

  @Before
  public void setupAccountsDao() {
    this.accounts = new Accounts(Jdbi.create(db.getTestDatabase()));
  }

  @Test
  public void testStore() throws SQLException, IOException {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM accounts WHERE number = ?");
    verifyStoredState(statement, "+14151112222", account);
  }

  @Test
  public void testStoreMulti() throws SQLException, IOException {
    Set<Device> devices = new HashSet<>();
    devices.add(generateDevice(1));
    devices.add(generateDevice(2));

    Account account = generateAccount("+14151112222", devices);

    accounts.create(account);

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM accounts WHERE number = ?");
    verifyStoredState(statement, "+14151112222", account);
  }

  @Test
  public void testRetrieve() {
    Set<Device> devicesFirst = new HashSet<>();
    devicesFirst.add(generateDevice(1));
    devicesFirst.add(generateDevice(2));

    Account accountFirst = generateAccount("+14151112222", devicesFirst);

    Set<Device> devicesSecond = new HashSet<>();
    devicesSecond.add(generateDevice(1));
    devicesSecond.add(generateDevice(2));

    Account accountSecond = generateAccount("+14152221111", devicesSecond);

    accounts.create(accountFirst);
    accounts.create(accountSecond);

    Optional<Account> retrievedFirst = accounts.get("+14151112222");
    Optional<Account> retrievedSecond = accounts.get("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", retrievedSecond.get(), accountSecond);
  }

  @Test
  public void testOverwrite() throws Exception {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM accounts WHERE number = ?");
    verifyStoredState(statement, "+14151112222", account);

    device = generateDevice(1);
    account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);
    verifyStoredState(statement, "+14151112222", account);
  }

  @Test
  public void testUpdate() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);

    device.setName("foobar");

    accounts.update(account);

    Optional<Account> retrieved = accounts.get("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", retrieved.get(), account);
  }

  @Test
  public void testRetrieveFrom() {
    List<Account> users = new ArrayList<>();

    for (int i=1;i<=100;i++) {
      Account account = generateAccount("+1" + String.format("%03d", i));
      users.add(account);
      accounts.create(account);
    }

    List<Account> retrieved = accounts.getAllFrom(10);
    assertThat(retrieved.size()).isEqualTo(10);

    for (int i=0;i<retrieved.size();i++) {
      verifyStoredState("+1" + String.format("%03d", (i + 1)), retrieved.get(i), users.get(i));
    }

    for (int j=0;j<9;j++) {
      retrieved = accounts.getAllFrom(retrieved.get(9).getNumber(), 10);
      assertThat(retrieved.size()).isEqualTo(10);

      for (int i=0;i<retrieved.size();i++) {
        verifyStoredState("+1" + String.format("%03d", (10 + (j * 10) + i + 1)), retrieved.get(i), users.get(10 + (j * 10) + i));
      }
    }
  }

  @Test
  public void testVacuum() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);
    accounts.vacuum();

    Optional<Account> retrieved = accounts.get("+14151112222");
    assertThat(retrieved.isPresent()).isTrue();

    verifyStoredState("+14151112222", retrieved.get(), account);
  }

  @Test
  public void testMissing() {
    Device  device  = generateDevice (1                                            );
    Account account = generateAccount("+14151112222", Collections.singleton(device));

    accounts.create(account);

    Optional<Account> retrieved = accounts.get("+11111111");
    assertThat(retrieved.isPresent()).isFalse();
  }


  private Device generateDevice(long id) {
    Random       random       = new Random(System.currentTimeMillis());
    SignedPreKey signedPreKey = new SignedPreKey(random.nextInt(), "testPublicKey-" + random.nextInt(), "testSignature-" + random.nextInt());
    return new Device(1, "testName-" + random.nextInt(), "testAuthToken-" + random.nextInt(), "testSalt-" + random.nextInt(), null, "testGcmId-" + random.nextInt(), "testApnId-" + random.nextInt(), "testVoipApnId-" + random.nextInt(), random.nextBoolean(), random.nextInt(), signedPreKey, random.nextInt(), random.nextInt(), "testUserAgent-" + random.nextInt(), random.nextBoolean());
  }

  private Account generateAccount(String number) {
    Device device = generateDevice(1);
    return generateAccount(number, Collections.singleton(device));
  }

  private Account generateAccount(String number, Set<Device> devices) {
    byte[]       unidentifiedAccessKey = new byte[16];
    Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte)random.nextInt(255));

    return new Account(number, devices, unidentifiedAccessKey);
  }

  private void verifyStoredState(PreparedStatement statement, String number, Account expecting)
      throws SQLException, IOException
  {
    statement.setString(1, number);

    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      String data = resultSet.getString("data");
      assertThat(data).isNotEmpty();

      Account result = new AccountRowMapper().map(resultSet, null);
      verifyStoredState(number, result, expecting);
    } else {
      throw new AssertionError("No data");
    }

    assertThat(resultSet.next()).isFalse();
  }

  private void verifyStoredState(String number, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(Arrays.equals(result.getUnidentifiedAccessKey().get(), expecting.getUnidentifiedAccessKey().get())).isTrue();

    for (Device expectingDevice : expecting.getDevices()) {
      Device resultDevice = result.getDevice(expectingDevice.getId()).get();
      assertThat(resultDevice.getApnId()).isEqualTo(expectingDevice.getApnId());
      assertThat(resultDevice.getGcmId()).isEqualTo(expectingDevice.getGcmId());
      assertThat(resultDevice.getLastSeen()).isEqualTo(expectingDevice.getLastSeen());
      assertThat(resultDevice.getSignedPreKey().getPublicKey()).isEqualTo(expectingDevice.getSignedPreKey().getPublicKey());
      assertThat(resultDevice.getSignedPreKey().getKeyId()).isEqualTo(expectingDevice.getSignedPreKey().getKeyId());
      assertThat(resultDevice.getSignedPreKey().getSignature()).isEqualTo(expectingDevice.getSignedPreKey().getSignature());
      assertThat(resultDevice.getFetchesMessages()).isEqualTo(expectingDevice.getFetchesMessages());
      assertThat(resultDevice.getUserAgent()).isEqualTo(expectingDevice.getUserAgent());
      assertThat(resultDevice.getName()).isEqualTo(expectingDevice.getName());
      assertThat(resultDevice.getCreated()).isEqualTo(expectingDevice.getCreated());
    }
  }




}
