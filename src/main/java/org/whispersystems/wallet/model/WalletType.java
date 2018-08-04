package org.whispersystems.wallet.model;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public enum WalletType {
    BTC, ETH, BTG;

    static final Map<String, WalletType> TYPE_BY_NAME = stream(values()).collect(toMap(Enum::name, identity()));

    public static WalletType of(String typeName) {
        WalletType walletType = TYPE_BY_NAME.get(typeName);
        if (walletType == null) {
            throw new IllegalArgumentException(String.format("WalletType with name %s is not supported!", typeName));
        }
        return walletType;
    }
}
