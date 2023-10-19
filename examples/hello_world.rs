use std::{fs, sync::Arc};

fn main() -> litemdb::Result<()> {
    // Path to database directory.
    let path = "./litemdb";

    // LMDB requires us to create the directory ourselves.
    fs::create_dir_all(path).expect("failed to create LMDB directory");

    // Apply some optimizations (optional).
    let flags = litemdb::environment::Flags::WRITE_MAP | litemdb::environment::Flags::NO_META_SYNC;

    // The maximum size of database in bytes (a high value is recommended).
    let map_size = 4096 * 4096 * 64;

    // Read & write permissions, see https://en.wikipedia.org/wiki/File-system_permissions
    let mode = 0o666;

    // Create and open the LMDB environment. Under the hood it will also create a
    // shared database handle so that we don't have to worry about it.
    let env = litemdb::Environment::open(path, flags, map_size, mode)?;

    // We use [`Arc`] because [`Environment`] is supposed to be shared between
    // threads.
    let env = Arc::new(env);

    let (key, data) = (b"hello_world", b"Hello, World!");

    // Begin a write transaction.
    let mut txn = env.begin_transaction(litemdb::transaction::Flags::empty())?;

    // Insert some data.
    txn.put(key, data)?;

    // Get that same data back.
    let view = txn.get(key)?.unwrap();

    // Verify it's the same.
    assert_eq!(view.as_ref(), data);

    // Print it.
    println!("{}", std::str::from_utf8(data.as_ref()).unwrap());

    // Delete it.
    txn.del(key)?;

    // Commit the transaction.
    txn.commit()?;

    Ok(())
}
