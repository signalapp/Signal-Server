package org.whispersystems.wallet.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WalletEntity {
    private String     phoneNumber;
    private WalletType walletType;
    private String     walletAddress;
}
