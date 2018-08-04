package org.whispersystems.wallet.dao;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.whispersystems.wallet.model.WalletEntity;
import org.whispersystems.wallet.model.WalletType;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.whispersystems.wallet.dao.WalletDao.PHONE_NUMBER;
import static org.whispersystems.wallet.dao.WalletDao.WALLET_ADDRESS;
import static org.whispersystems.wallet.dao.WalletDao.WALLET_TYPE;

public class WalletMapper implements ResultSetMapper<WalletEntity> {
    @Override
    public WalletEntity map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
        return new WalletEntity(
                resultSet.getString(PHONE_NUMBER),
                WalletType.of(resultSet.getString(WALLET_TYPE)),
                resultSet.getString(WALLET_ADDRESS)
        );
    }
}
