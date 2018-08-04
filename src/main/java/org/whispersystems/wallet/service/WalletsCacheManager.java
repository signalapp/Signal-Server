package org.whispersystems.wallet.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.wallet.model.WalletDto;
import org.whispersystems.wallet.model.WalletType;

import java.util.List;
import java.util.Optional;

//TODO implement cache functionality when main flow is working
@RequiredArgsConstructor
public class WalletsCacheManager {
    private final ObjectMapper        mapper;
    private final ReplicatedJedisPool cacheClient;

    public Optional<WalletDto> loadByPhoneNumberAndType(String phoneNumber,
                                                        WalletType walletType) {
        return Optional.empty();
    }

    public WalletDto set(WalletDto wallet) {
        return wallet;
    }

    public List<WalletDto> set(List<WalletDto> wallets) {
        return wallets;
    }
}
