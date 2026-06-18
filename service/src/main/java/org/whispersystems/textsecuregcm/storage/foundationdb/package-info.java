/// These classes manage message storage in a set of FoundationDB databases.
///
/// # Schema
///
/// ## Sharding
/// The messages database is expected to be larger than a single FoundationDB cluster
/// can handle, and is therefore sharded by recipient ACI. The shard for a
/// given recipient is identified by a [consistent
/// hash][com.google.common.hash.Hashing#consistentHash()] applied to the low
/// 64 bits of the ACI.
///
/// ## Messages
/// * Root subspace `M`
/// * Managed by [FoundationDbMessageStore]
/// * Sharded by ACI: database `i` of `n` will only contain account subspaces for
///   UUIDs where `consistentHash(u.getLeastSignificantBits(), n) == i`
/// * Contains mailboxes and associated bookkeeping information for each user
/// * Schema:
/// ```
/// + "M" (messages subspace)
/// | + {aci (UUID)} (account subspace)
/// | | * "l" => messages-available watch key, versionstamp of last message inserted when any recipient
/// | |   device was present
/// | | + {device ID (byte)} (device subspace)
/// | | | * "p" => presence: server-id || last-seen-time
/// | | | + "Q" (device message queue subspace)
/// | | | | * versionstamp => serialized envelope
/// ```
///
/// ## Versionstamp Clock
/// * Root subspace `V`
/// * Managed by [VersionstampClock]
/// * Exists in every messages database; (necessarily) independent for each
/// * Global metadata tracking a mapping from time to versionstamp for the expired-message cleaner
/// * Schema:
/// ```
/// + "V" (versionstamp-clock subspace)
/// | + unix time => versionstamp
/// ```
package org.whispersystems.textsecuregcm.storage.foundationdb;
