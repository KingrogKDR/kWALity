# WAL

## Lifecycle

Open() -> usable
Close() -> unusable

## Castagnoli Checksum

For error detection, the WAL uses CRC32C / Castagnoli because it is particularly good for short bursts of corruption. It also has hardware acceleration on many CPUs, which makes it very fast. It is heavily used in storage systems, and we see them in LevelDB, RocksDB, ext4, etc.
