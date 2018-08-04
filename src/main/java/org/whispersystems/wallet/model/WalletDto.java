package org.whispersystems.wallet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class WalletDto {
    public static final int MEM_CACHE_VERSION = 1;
    @JsonProperty
    String     phoneNumber;
    @JsonProperty
    WalletType walletType;
    @JsonProperty
    String     walletAddress;

    public static WalletDto from(WalletEntity walletEntity) {
        return new WalletDto(walletEntity.getPhoneNumber(),
                             walletEntity.getWalletType(),
                             walletEntity.getWalletAddress());
    }

    public WalletEntity toEntity() {
        return new WalletEntity(this.getPhoneNumber(),
                                this.getWalletType(),
                                this.getWalletAddress());
    }
}
