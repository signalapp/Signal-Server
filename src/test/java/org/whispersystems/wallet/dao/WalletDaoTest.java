package org.whispersystems.wallet.dao;

import com.github.arteam.jdit.DBIRunner;
import com.github.arteam.jdit.annotations.DBIHandle;
import com.github.arteam.jdit.annotations.DataSet;
import com.github.arteam.jdit.annotations.TestedSqlObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skife.jdbi.v2.Handle;
import org.whispersystems.wallet.model.WalletEntity;
import org.whispersystems.wallet.model.WalletType;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.whispersystems.wallet.model.WalletType.BTC;
import static org.whispersystems.wallet.model.WalletType.ETH;

@RunWith(DBIRunner.class)
public class WalletDaoTest {
    private static final WalletType WALLET_TYPE    = BTC;
    private static final String     PHONE_NUMBER   = "123456";
    private static final String     WALLET_ADDRESS = "walletAddress";
    @TestedSqlObject
    private              WalletDao  walletDao;
    @DBIHandle
    private              Handle     handle;

    @Test
    public void shouldSaveNewWallet() {
        WalletEntity walletEntity = new WalletEntity(PHONE_NUMBER, WALLET_TYPE, WALLET_ADDRESS);

        walletDao.save(walletEntity);

        List<Map<String, Object>> rows         = handle.select("SELECT * FROM wallets where phone_number=?", PHONE_NUMBER);
        Map<String, Object>       actualResult = rows.get(0);

        assertEquals(PHONE_NUMBER,
                     actualResult.get("phone_number"));
        assertEquals(WALLET_TYPE.name(),
                     actualResult.get("wallet_type"));
        assertEquals(WALLET_ADDRESS,
                     actualResult.get("wallet_address"));
    }

    @DataSet("db/wallets.sql")
    @Test
    public void shouldFindByNumber() {
        String secondPhoneNumber = "654321";
        WalletEntity expectedWalletEntity = new WalletEntity(secondPhoneNumber, ETH, "eth_wallet_address_654321");

        List<WalletEntity> byPhoneNumber = walletDao.findByPhoneNumber(PHONE_NUMBER);

        assertEquals(2, byPhoneNumber.size());

        assertTrue(Stream.of(BTC, ETH)
                         .allMatch(type -> byPhoneNumber.stream()
                                                        .anyMatch(walletEntity -> walletEntity.getWalletType() == type)));

        List<WalletEntity> bySecondPhoneNumber = walletDao.findByPhoneNumber(secondPhoneNumber);

        assertEquals(1, bySecondPhoneNumber.size());
        assertEquals(expectedWalletEntity, bySecondPhoneNumber.get(0));
    }

    @DataSet("db/wallets.sql")
    @Test
    public void shouldReturnEmptyListForFindByNumberIfNoData() {
        String phoneNumber = "321321";

        List<WalletEntity> byPhoneNumber = walletDao.findByPhoneNumber(phoneNumber);

        assertTrue(byPhoneNumber.isEmpty());
    }

    @DataSet("db/wallets.sql")
    @Test
    public void shouldFindByNumberAndWalletType() {
        WalletEntity firstExpectedWalletEntity = new WalletEntity(PHONE_NUMBER, ETH, "eth_wallet_address_123456");
        WalletEntity secondExpectedWalletEntity = new WalletEntity(PHONE_NUMBER, BTC, "btc_wallet_address_123456");

        WalletEntity firstActualWalletEntity = walletDao.findByPhoneNumberAndWalletType(PHONE_NUMBER, ETH);
        WalletEntity secondActualWalletEntity = walletDao.findByPhoneNumberAndWalletType(PHONE_NUMBER, BTC);

        assertEquals(firstExpectedWalletEntity, firstActualWalletEntity);
        assertEquals(secondExpectedWalletEntity, secondActualWalletEntity);
    }



    @DataSet("db/wallets.sql")
    @Test
    public void shouldReturnNullForFindByNumberAndWalletTypeIfNoData() {
        String phoneNumber = "321321";

        WalletEntity byPhoneNumberAndWalletType = walletDao.findByPhoneNumberAndWalletType(phoneNumber, ETH);

        assertNull(byPhoneNumberAndWalletType);
    }
}