package org.whispersystems.wallet.dao;

import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.whispersystems.wallet.model.WalletEntity;

import static org.whispersystems.wallet.dao.WalletDao.PHONE_NUMBER;
import static org.whispersystems.wallet.dao.WalletDao.WALLET_ADDRESS;
import static org.whispersystems.wallet.dao.WalletDao.WALLET_TYPE;

public class WalletBinderFactory implements BinderFactory<WalletBinder> {
    @Override
    public Binder<WalletBinder, WalletEntity> build(WalletBinder annotation) {
        return (sql, walletBinder, walletEntity) -> {
            sql.bind(PHONE_NUMBER, walletEntity.getPhoneNumber());
            sql.bind(WALLET_TYPE, walletEntity.getWalletType());
            sql.bind(WALLET_ADDRESS, walletEntity.getWalletAddress());
        };
    }
}
