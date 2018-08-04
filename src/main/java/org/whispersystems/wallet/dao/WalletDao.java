package org.whispersystems.wallet.dao;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.whispersystems.wallet.model.WalletEntity;
import org.whispersystems.wallet.model.WalletType;

import java.util.List;

public interface WalletDao {
    String PHONE_NUMBER   = "phone_number";
    String WALLET_TYPE    = "wallet_type";
    String WALLET_ADDRESS = "wallet_address";

    @SqlUpdate("INSERT INTO wallets (" + PHONE_NUMBER + ", " + WALLET_TYPE + ", " + WALLET_ADDRESS + ") " +
               "VALUES (:" + PHONE_NUMBER + ", :" + WALLET_TYPE + ", :" + WALLET_ADDRESS + ")")
    void save(@WalletBinder WalletEntity walletEntity);

    @Mapper(WalletMapper.class)
    @SqlQuery("SELECT * FROM wallets WHERE " + PHONE_NUMBER + " = :" + PHONE_NUMBER)
    List<WalletEntity> findByPhoneNumber(@Bind(PHONE_NUMBER) String phoneNumber);

    @Mapper(WalletMapper.class)
    @SqlQuery("SELECT * FROM wallets WHERE " + PHONE_NUMBER + " = :" + PHONE_NUMBER +
              " AND " + WALLET_TYPE + " = :" + WALLET_TYPE)
    WalletEntity findByPhoneNumberAndWalletType(@Bind(PHONE_NUMBER) String phoneNumber,
                                                @Bind(WALLET_TYPE) WalletType walletType);

}
