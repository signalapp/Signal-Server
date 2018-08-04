package org.whispersystems.wallet.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.whispersystems.wallet.dao.WalletDao;
import org.whispersystems.wallet.model.WalletDto;
import org.whispersystems.wallet.model.WalletEntity;
import org.whispersystems.wallet.model.WalletType;

import java.util.List;
import java.util.Optional;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

@Slf4j
@RequiredArgsConstructor
public class WalletsManager {
    private final WalletDao           walletDao;
    private final WalletsCacheManager cacheManager;

    public Optional<WalletDto> getWallet(String phoneNumber,
                                         WalletType walletType) {
        log.debug("Trying to load wallet for number: {}, type: {} from cache.");
        Optional<WalletDto> walletDto = cacheManager.loadByPhoneNumberAndType(phoneNumber, walletType);
        if (walletDto.isPresent()) {
            log.debug("Wallet {} has been loaded from cache.", walletDto);
            return walletDto;
        }
        return loadFromDataBaseAndRefreshCache(phoneNumber, walletType);
    }

    public List<WalletDto> getAllWallets(String phoneNumber) {
        log.debug("Trying to load all wallets for number: {} from the database.", phoneNumber);
        List<WalletEntity> walletEntities = walletDao.findByPhoneNumber(phoneNumber);
        if (isNullOrEmpty(walletEntities)) {
            log.debug("No wallets has been found for number: {} .", phoneNumber);
            return emptyList();
        }
        log.debug("All wallets for number: {} has been loaded from the database, updating cache.", phoneNumber);
        List<WalletDto> walletDtos = walletEntities.stream()
                                                   .map(WalletDto::from)
                                                   .collect(toList());
        cacheManager.set(walletDtos);
        log.debug("All wallets: {} for number: {} has been added to cache.", walletDtos, phoneNumber);
        return walletDtos;
    }

    public void addWallet(WalletDto walletDto) {
        log.debug("Trying to save wallet {} to the database.", walletDto);
        walletDao.save(walletDto.toEntity());
        log.debug("Wallet {} has been saved to the database, updating cache.", walletDto);
        cacheManager.set(walletDto);
        log.debug("Wallet {} has been added to cache.", walletDto);
    }

    private Optional<WalletDto> loadFromDataBaseAndRefreshCache(String phoneNumber, WalletType walletType) {
        log.debug("Trying to load wallet for number: {}, type: {} from the database.", phoneNumber, walletType);
        Optional<WalletDto> walletDto = Optional.of(walletDao.findByPhoneNumberAndWalletType(phoneNumber, walletType))
                                                .map(WalletDto::from)
                                                .map(cacheManager::set);
        if (walletDto.isPresent()) {
            log.debug("Wallet {}, was loaded from db and cache was updated.", walletDto);
        } else {
            log.debug("Wallet for number: {}, type: {} hasn't been found.", phoneNumber, walletType);
        }
        return walletDto;
    }

}
