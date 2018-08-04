package org.whispersystems.wallet.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.whispersystems.wallet.service.WalletsManager;
@Slf4j
@RequiredArgsConstructor
public class WalletController {
    private final WalletsManager walletsManager;
}
