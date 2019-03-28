package org.whispersystems.textsecuregcm.tests.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class KeysTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  @Test
  public void testPopulateKeys() throws SQLException {
    DataSource dataSource = db.getTestDatabase();
    DBI        dbi        = new DBI(dataSource);
    Keys       keys       = dbi.onDemand(Keys.class);

    List<PreKey> deviceOnePreKeys = new LinkedList<>();
    List<PreKey> deviceTwoPreKeys = new LinkedList<>();

    List<PreKey> oldAnotherDeviceOnePrKeys = new LinkedList<>();
    List<PreKey> anotherDeviceOnePreKeys = new LinkedList<>();
    List<PreKey> anotherDeviceTwoPreKeys = new LinkedList<>();

    for (int i=1;i<=100;i++) {
      deviceOnePreKeys.add(new PreKey(i, "+14152222222Device1PublicKey" + i));
      deviceTwoPreKeys.add(new PreKey(i, "+14152222222Device2PublicKey" + i));
    }

    for (int i=1;i<=100;i++) {
      oldAnotherDeviceOnePrKeys.add(new PreKey(i, "OldPublicKey" + i));
      anotherDeviceOnePreKeys.add(new PreKey(i, "+14151111111Device1PublicKey" + i));
      anotherDeviceTwoPreKeys.add(new PreKey(i, "+14151111111Device2PublicKey" + i));
    }

    keys.store("+14152222222", 1, deviceOnePreKeys);
    keys.store("+14152222222", 2, deviceTwoPreKeys);

    keys.store("+14151111111", 1, oldAnotherDeviceOnePrKeys);
    keys.store("+14151111111", 1, anotherDeviceOnePreKeys);
    keys.store("+14151111111", 2, anotherDeviceTwoPreKeys);

    PreparedStatement statement = dataSource.getConnection().prepareStatement("SELECT * FROM keys WHERE number = ? AND device_id = ? ORDER BY key_id");
    verifyStoredState(statement, "+14152222222", 1);
    verifyStoredState(statement, "+14152222222", 2);
    verifyStoredState(statement, "+14151111111", 1);
    verifyStoredState(statement, "+14151111111", 2);
  }

  @Test
  public void testKeyCount() throws SQLException {
    DataSource dataSource = db.getTestDatabase();
    DBI        dbi        = new DBI(dataSource);
    Keys       keys       = dbi.onDemand(Keys.class);

    List<PreKey> deviceOnePreKeys = new LinkedList<>();

    for (int i=1;i<=100;i++) {
      deviceOnePreKeys.add(new PreKey(i, "+14152222222Device1PublicKey" + i));
    }


    keys.store("+14152222222", 1, deviceOnePreKeys);

    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(100);
  }

  @Test
  public void testGetForDevice() {
    DataSource dataSource = db.getTestDatabase();
    DBI        dbi        = new DBI(dataSource);
    Keys       keys       = dbi.onDemand(Keys.class);

    List<PreKey> deviceOnePreKeys = new LinkedList<>();
    List<PreKey> deviceTwoPreKeys = new LinkedList<>();

    List<PreKey> anotherDeviceOnePreKeys = new LinkedList<>();
    List<PreKey> anotherDeviceTwoPreKeys = new LinkedList<>();

    for (int i=1;i<=100;i++) {
      deviceOnePreKeys.add(new PreKey(i, "+14152222222Device1PublicKey" + i));
      deviceTwoPreKeys.add(new PreKey(i, "+14152222222Device2PublicKey" + i));
    }

    for (int i=1;i<=100;i++) {
      anotherDeviceOnePreKeys.add(new PreKey(i, "+14151111111Device1PublicKey" + i));
      anotherDeviceTwoPreKeys.add(new PreKey(i, "+14151111111Device2PublicKey" + i));
    }

    keys.store("+14152222222", 1, deviceOnePreKeys);
    keys.store("+14152222222", 2, deviceTwoPreKeys);

    keys.store("+14151111111", 1, anotherDeviceOnePreKeys);
    keys.store("+14151111111", 2, anotherDeviceTwoPreKeys);


    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(100);
    List<KeyRecord> records = keys.get("+14152222222", 1);

    assertThat(records.size()).isEqualTo(1);
    assertThat(records.get(0).getKeyId()).isEqualTo(1);
    assertThat(records.get(0).getPublicKey()).isEqualTo("+14152222222Device1PublicKey1");
    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(99);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(100);
    assertThat(keys.getCount("+14151111111", 1)).isEqualTo(100);
    assertThat(keys.getCount("+14151111111", 2)).isEqualTo(100);

    records = keys.get("+14152222222", 1);

    assertThat(records.size()).isEqualTo(1);
    assertThat(records.get(0).getKeyId()).isEqualTo(2);
    assertThat(records.get(0).getPublicKey()).isEqualTo("+14152222222Device1PublicKey2");
    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(98);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(100);
    assertThat(keys.getCount("+14151111111", 1)).isEqualTo(100);
    assertThat(keys.getCount("+14151111111", 2)).isEqualTo(100);

    records = keys.get("+14152222222", 2);

    assertThat(records.size()).isEqualTo(1);
    assertThat(records.get(0).getKeyId()).isEqualTo(1);
    assertThat(records.get(0).getPublicKey()).isEqualTo("+14152222222Device2PublicKey1");
    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(98);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(99);
    assertThat(keys.getCount("+14151111111", 1)).isEqualTo(100);
    assertThat(keys.getCount("+14151111111", 2)).isEqualTo(100);
  }

  @Test
  public void testGetForAllDevices() {
    DataSource dataSource = db.getTestDatabase();
    DBI        dbi        = new DBI(dataSource);
    Keys       keys       = dbi.onDemand(Keys.class);

    List<PreKey> deviceOnePreKeys = new LinkedList<>();
    List<PreKey> deviceTwoPreKeys = new LinkedList<>();

    List<PreKey> anotherDeviceOnePreKeys   = new LinkedList<>();
    List<PreKey> anotherDeviceTwoPreKeys   = new LinkedList<>();
    List<PreKey> anotherDeviceThreePreKeys = new LinkedList<>();

    for (int i=1;i<=100;i++) {
      deviceOnePreKeys.add(new PreKey(i, "+14152222222Device1PublicKey" + i));
      deviceTwoPreKeys.add(new PreKey(i, "+14152222222Device2PublicKey" + i));
    }

    for (int i=1;i<=100;i++) {
      anotherDeviceOnePreKeys.add(new PreKey(i, "+14151111111Device1PublicKey" + i));
      anotherDeviceTwoPreKeys.add(new PreKey(i, "+14151111111Device2PublicKey" + i));
      anotherDeviceThreePreKeys.add(new PreKey(i, "+14151111111Device3PublicKey" + i));
    }

    keys.store("+14152222222", 1, deviceOnePreKeys);
    keys.store("+14152222222", 2, deviceTwoPreKeys);

    keys.store("+14151111111", 1, anotherDeviceOnePreKeys);
    keys.store("+14151111111", 2, anotherDeviceTwoPreKeys);
    keys.store("+14151111111", 3, anotherDeviceThreePreKeys);


    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(100);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(100);

    List<KeyRecord> records = keys.get("+14152222222");

    assertThat(records.size()).isEqualTo(2);
    assertThat(records.get(0).getKeyId()).isEqualTo(1);
    assertThat(records.get(1).getKeyId()).isEqualTo(1);

    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14152222222Device1PublicKey1"))).isTrue();
    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14152222222Device2PublicKey1"))).isTrue();

    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(99);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(99);

    records = keys.get("+14152222222");

    assertThat(records.size()).isEqualTo(2);
    assertThat(records.get(0).getKeyId()).isEqualTo(2);
    assertThat(records.get(1).getKeyId()).isEqualTo(2);

    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14152222222Device1PublicKey2"))).isTrue();
    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14152222222Device2PublicKey2"))).isTrue();

    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(98);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(98);


    records = keys.get("+14151111111");

    assertThat(records.size()).isEqualTo(3);
    assertThat(records.get(0).getKeyId()).isEqualTo(1);
    assertThat(records.get(1).getKeyId()).isEqualTo(1);
    assertThat(records.get(2).getKeyId()).isEqualTo(1);

    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14151111111Device1PublicKey1"))).isTrue();
    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14151111111Device2PublicKey1"))).isTrue();
    assertThat(records.stream().anyMatch(record -> record.getPublicKey().equals("+14151111111Device3PublicKey1"))).isTrue();

    assertThat(keys.getCount("+14151111111", 1)).isEqualTo(99);
    assertThat(keys.getCount("+14151111111", 2)).isEqualTo(99);
    assertThat(keys.getCount("+14151111111", 3)).isEqualTo(99);
  }

  @Test
  public void testGetForAllDevicesParallel() throws InterruptedException {
    DataSource dataSource = db.getTestDatabase();
    DBI        dbi        = new DBI(dataSource);
    Keys       keys       = dbi.onDemand(Keys.class);

    List<PreKey> deviceOnePreKeys = new LinkedList<>();
    List<PreKey> deviceTwoPreKeys = new LinkedList<>();

    for (int i=1;i<=100;i++) {
      deviceOnePreKeys.add(new PreKey(i, "+14152222222Device1PublicKey" + i));
      deviceTwoPreKeys.add(new PreKey(i, "+14152222222Device2PublicKey" + i));
    }

    keys.store("+14152222222", 1, deviceOnePreKeys);
    keys.store("+14152222222", 2, deviceTwoPreKeys);

    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(100);
    assertThat(keys.getCount("+14152222222", 2)).isEqualTo(100);

    List<Thread> threads = new LinkedList<>();

    for (int i=0;i<50;i++) {
      Thread thread = new Thread(() -> {
        List<KeyRecord> results = keys.get("+14152222222");
        assertThat(results.size()).isEqualTo(2);
      });
      thread.start();
      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }

    assertThat(keys.getCount("+14152222222", 1)).isEqualTo(50);
    assertThat(keys.getCount("+14152222222",2)).isEqualTo(50);
  }


  @Test
  public void testEmptyKeyGet() {
    DBI  dbi  = new DBI(db.getTestDatabase());
    Keys keys = dbi.onDemand(Keys.class);

    List<KeyRecord> records = keys.get("+14152222222");

    assertThat(records.isEmpty()).isTrue();
  }


  private void verifyStoredState(PreparedStatement statement, String number, int deviceId) throws SQLException {
    statement.setString(1, number);
    statement.setInt(2, deviceId);

    ResultSet resultSet = statement.executeQuery();
    int       rowCount  = 1;

    while (resultSet.next()) {
      long   keyId     = resultSet.getLong("key_id");
      String publicKey = resultSet.getString("public_key");


      assertThat(keyId).isEqualTo(rowCount);
      assertThat(publicKey).isEqualTo(number + "Device" + deviceId + "PublicKey" + rowCount);

      rowCount++;
    }

    resultSet.close();

    assertThat(rowCount).isEqualTo(101);

  }

}
