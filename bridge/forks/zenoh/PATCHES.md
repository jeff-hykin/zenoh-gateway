# zenoh: patches on top of zenoh 1.10.1

Copy of the [zenoh](https://crates.io/crates/zenoh) 1.10.1 crate (Eclipse zenoh, EPL-2.0 OR
Apache-2.0, © ZettaScale Technology), used through `[patch.crates-io]`. Every change is marked
`zenoh-web patch` in the source.

1. **The admin space replies after releasing the routing tables' lock** (`src/net/runtime/adminspace.rs`:
   `resources_data`, `linkstate_data`, `route_successor`). Bug: these handlers replied while holding
   the tables' read lock, and sending a reply takes that read lock again. A declaration waiting for
   the write lock in between blocks the second read (std's `RwLock` lets a waiting writer go first),
   so both wait forever and every zenoh and tokio thread piles up behind them. zenoh-web's
   `listTopics` queries `@/*/*/subscriber/**`, so a page polling it while another subscribed hung
   the whole process. Change: collect the replies under the lock, release it, then reply. Same fix
   as the open upstream PR eclipse-zenoh/zenoh#2619; drop this fork once a release has it.
2. **`ZENOH_WEB_PATCHES`** (`src/lib.rs`): a constant zenoh-web reads at compile time, so a
   dependent that forgot the `[patch.crates-io]` entry gets a compile error rather than the deadlock.
