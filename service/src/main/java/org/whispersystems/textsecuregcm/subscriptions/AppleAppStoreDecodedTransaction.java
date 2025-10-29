package org.whispersystems.textsecuregcm.subscriptions;

import com.apple.itunes.storekit.model.JWSRenewalInfoDecodedPayload;
import com.apple.itunes.storekit.model.JWSTransactionDecodedPayload;
import com.apple.itunes.storekit.model.LastTransactionsItem;

/**
 * A decoded and validated storekit transaction
 *
 * @param signedTransaction The transaction
 * @param transaction The transaction info with a validated signature
 * @param renewalInfo The renewal info with a validated signature
 */
record AppleAppStoreDecodedTransaction(
    LastTransactionsItem signedTransaction,
    JWSTransactionDecodedPayload transaction,
    JWSRenewalInfoDecodedPayload renewalInfo) {}
