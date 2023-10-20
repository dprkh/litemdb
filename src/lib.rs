#![forbid(
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_safety_comment,
    clippy::unnecessary_safety_doc,
    rustdoc::broken_intra_doc_links,
    rustdoc::private_intra_doc_links
)]
#![allow(clippy::missing_const_for_fn)]

use std::{
    ffi::CStr,
    fmt::{Debug, Formatter},
};

mod sys {
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        non_upper_case_globals,
        non_camel_case_types,
        non_snake_case,
        unused
    )]

    include!(concat!(env!("OUT_DIR"), "/lmdb.rs"));
}

pub use environment::Environment;
pub use transaction::Transaction;

#[derive(Copy, Clone)]
pub struct Error(i32);

impl Debug for Error {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // SAFETY: [`sys::mdb_strerror`] always returns a valid pointer
        let str = unsafe { CStr::from_ptr(sys::mdb_strerror(self.0)) };
        Debug::fmt(str, f)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub mod environment {
    use std::{
        ffi::CString,
        mem::MaybeUninit,
        ptr::{null, null_mut},
        sync::Arc,
    };

    use crate::{
        sys,
        transaction::{self, Transaction},
        Error, Result,
    };

    bitflags::bitflags! {
        #[derive(Copy, Clone, Debug)]
        pub struct Flags: u32 {
            /// Use a writeable memory map. This is faster and uses fewer mallocs, but
            /// loses protection from application bugs like wild pointer writes and
            /// other bad updates into the database.
            const WRITE_MAP = sys::MDB_WRITEMAP;
            /// Flush system buffers to disk only once per transaction, omit the
            /// metadata flush. Defer that until the system flushes files to disk. This
            /// optimization maintains database integrity, but a system crash may undo
            /// the last committed transaction. I.e. it preserves the ACI (atomicity,
            /// consistency, isolation) but not D (durability) database property.
            const NO_META_SYNC = sys::MDB_NOMETASYNC;
        }
    }

    pub struct Environment {
        env: *mut sys::MDB_env,
        // Eagerly open a database during [`Environment`] construction and keep it here to avoid
        // dealing with LMDB's constraints.
        pub(crate) dbi: sys::MDB_dbi,
    }

    // SAFETY: LMDB environment is thread-safe
    unsafe impl Send for Environment {}
    // SAFETY: LMDB environment is thread-safe
    unsafe impl Sync for Environment {}

    impl Environment {
        /// [`Creates`][0] an LMDB environment and [`opens`][1] it.
        ///
        /// # Parameters
        ///
        /// * `path` - the directory in which the database files reside. This
        ///   directory must already exist and be writable.
        /// * `flags` - special options for this environment.
        /// * `map_size` - the size of the memory map to use for this
        ///   environment.
        /// * `mode` - the UNIX permissions to set on created files and
        ///   semaphores. This parameter is ignored on Windows.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// # Panics
        ///
        /// * Panics if `path` contains a null byte.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__mdb.html#gaad6be3d8dcd4ea01f8df436f41d158d4
        /// [1]: http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
        /// [2]: http://www.lmdb.tech/doc/group__mdb.html#ga4366c43ada8874588b6a62fbda2d1e95
        #[inline]
        pub fn open(path: &str, flags: Flags, map_size: usize, mode: u32) -> Result<Self> {
            let mut env = MaybeUninit::uninit();
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_env_create(env.as_mut_ptr()) };
            if r != 0 {
                return Err(Error(r));
            }
            // SAFETY: should have been initialized by the [`sys::mdb_env_create`] call
            let env = unsafe { env.assume_init() };
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_env_set_mapsize(env, map_size) };
            if r != 0 {
                return Err(Error(r));
            }
            let path = CString::new(path).expect("invalid `path` value");
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_env_open(env, path.as_ptr(), flags.bits(), mode) };
            if r != 0 {
                // SAFETY: `env` is not used after this call, so it's safe to close it
                unsafe { sys::mdb_env_close(env) };
                return Err(Error(r));
            }
            let mut txn = MaybeUninit::uninit();
            // SAFETY: the ffi call is immediately followed by an error check
            let r =
                unsafe { sys::mdb_txn_begin(env, null_mut(), sys::MDB_RDONLY, txn.as_mut_ptr()) };
            if r != 0 {
                // SAFETY: `env` is not used after this call, so it's safe to close it
                unsafe { sys::mdb_env_close(env) };
                return Err(Error(r));
            }
            // SAFETY: should have been initialized by the [`sys::mdb_txn_begin`] call
            let txn = unsafe { txn.assume_init() };
            let mut dbi = MaybeUninit::uninit();
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_dbi_open(txn, null(), 0, dbi.as_mut_ptr()) };
            if r != 0 {
                // SAFETY: `env` is not used after this call, so it's safe to close it
                unsafe { sys::mdb_env_close(env) };
                return Err(Error(r));
            }
            // SAFETY: should have been initialized by the [`sys::mdb_dbi_open`] call
            let dbi = unsafe { dbi.assume_init() };
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_txn_commit(txn) };
            if r != 0 {
                // SAFETY: `env` is not used after this call, so it's safe to close it
                unsafe { sys::mdb_env_close(env) };
                return Err(Error(r));
            }
            Ok(Self { env, dbi })
        }

        /// [`Creates`][0] a transaction with specified
        /// [`Flags`][transaction::Flags]. If the returned [`Transaction`] is
        /// dropped without being [`aborted`][1] or [`committed`][2] then its
        /// destructor will automatically [`abort`][1] it.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// # Panics
        ///
        /// * Panics if the calling thread already has another open
        ///   [`Transaction`].
        ///
        /// [0]: http://www.lmdb.tech/doc/group__internal.html#gaec09fc4062fc4d99882f7f7256570bdb
        /// [1]: Transaction::abort
        /// [2]: Transaction::commit
        #[inline]
        pub fn begin_transaction(
            self: &Arc<Self>,
            flags: transaction::Flags,
        ) -> Result<Transaction> {
            Transaction::begin(self.clone(), flags)
        }

        #[inline]
        #[must_use]
        pub(crate) fn as_raw_ptr(&self) -> *mut sys::MDB_env {
            self.env
        }
    }

    impl Drop for Environment {
        #[inline]
        fn drop(&mut self) {
            // SAFETY: all resources should be closed at this point, so it's safe to close
            // this [`Environment`]
            unsafe { sys::mdb_env_close(self.as_raw_ptr()) }
        }
    }
}

pub mod transaction {
    use std::{
        cell::Cell,
        mem::MaybeUninit,
        ptr::{addr_of, null_mut},
        slice,
        sync::Arc,
    };

    use crate::{environment::Environment, sys, Error, Result};

    pub struct DataView<'a> {
        /// We hold a shared reference to [`Transaction`] and thus guarantee
        /// that no operation that requires unique access to it will
        /// occur and that it won't be discarded.
        ///
        /// ```compile_fail
        /// use std::sync::Arc;
        ///
        /// fn test(env: Arc<litemdb::Environment>) -> litemdb::Result<()> {
        ///     let mut txn = env.begin_transaction(litemdb::transaction::Flags::empty())?;
        ///     if let Some(view) = txn.get(b"key")? {
        ///         txn.del(b"key")?;
        ///         // compiler error
        ///         println!("{:?}", view.as_ref());
        ///     }
        ///     Ok(())
        /// }
        /// ```
        _txn: &'a Transaction,
        data: &'a [u8],
    }

    impl<'a> AsRef<[u8]> for DataView<'a> {
        #[inline]
        #[must_use]
        fn as_ref(&self) -> &'a [u8] {
            self.data
        }
    }

    bitflags::bitflags! {
        #[derive(Copy, Clone, Debug)]
        pub struct Flags: u32 {
            /// This transaction will not perform any write operations.
            const READ_ONLY = sys::MDB_RDONLY;
        }
    }

    thread_local! {
        static ACTIVE_TXN: Cell<bool> = const { Cell::new(false) };
    }

    pub struct Transaction {
        // All [`Transaction`] objects must hold an [`Arc<Environment>`] to prevent the LMDB
        // environment from closing.
        env: Arc<Environment>,
        is_discarded: bool,
        txn: *mut sys::MDB_txn,
    }

    impl Transaction {
        #[inline]
        pub(crate) fn begin(env: Arc<Environment>, flags: Flags) -> Result<Self> {
            ACTIVE_TXN.with(|cell| {
                assert!(
                    !cell.replace(true),
                    "A thread may only have a single transaction at a time."
                );
            });
            let mut txn = MaybeUninit::uninit();
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe {
                sys::mdb_txn_begin(env.as_raw_ptr(), null_mut(), flags.bits(), txn.as_mut_ptr())
            };
            if r != 0 {
                return Err(Error(r));
            }
            // SAFETY: should have been initialized by the [`sys::mdb_txn_begin`] call
            let txn = unsafe { txn.assume_init() };
            Ok(Self {
                env,
                txn,
                is_discarded: false,
            })
        }

        /// [`Gets`][0] an item from database.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d
        #[inline]
        pub fn get(&self, key: &[u8]) -> Result<Option<DataView>> {
            let key = sys::MDB_val {
                mv_data: key.as_ptr().cast_mut().cast(),
                mv_size: key.len(),
            };
            let mut data = MaybeUninit::uninit();
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe {
                sys::mdb_get(
                    self.as_raw_ptr(),
                    self.env.dbi,
                    addr_of!(key).cast_mut(),
                    data.as_mut_ptr(),
                )
            };
            if r == sys::MDB_NOTFOUND {
                return Ok(None);
            }
            if r != 0 {
                return Err(Error(r));
            }
            // SAFETY: should have been initialized by the [`sys::mdb_get`] call
            let data = unsafe { data.assume_init() };
            // SAFETY: values returned from database are valid until a subsequent update
            // operation, or the end of the transaction
            let data = unsafe { slice::from_raw_parts(data.mv_data.cast(), data.mv_size) };
            Ok(Some(DataView { _txn: self, data }))
        }

        /// [`Puts`][0] an item into database.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0
        #[inline]
        pub fn put(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
            let key = sys::MDB_val {
                mv_data: key.as_ptr().cast_mut().cast(),
                mv_size: key.len(),
            };
            let data = sys::MDB_val {
                mv_data: data.as_ptr().cast_mut().cast(),
                mv_size: data.len(),
            };
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe {
                sys::mdb_put(
                    self.as_raw_ptr(),
                    self.env.dbi,
                    addr_of!(key).cast_mut(),
                    addr_of!(data).cast_mut(),
                    0,
                )
            };
            if r != 0 {
                return Err(Error(r));
            }
            Ok(())
        }

        /// [`Deletes`][0] an item from database.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0
        #[inline]
        pub fn del(&mut self, key: &[u8]) -> Result<bool> {
            let key = sys::MDB_val {
                mv_data: key.as_ptr().cast_mut().cast(),
                mv_size: key.len(),
            };
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe {
                sys::mdb_del(
                    self.as_raw_ptr(),
                    self.env.dbi,
                    addr_of!(key).cast_mut(),
                    null_mut(),
                )
            };
            if r == sys::MDB_NOTFOUND {
                return Ok(false);
            }
            if r != 0 {
                return Err(Error(r));
            }
            Ok(true)
        }

        /// [`Aborts`][0] all operations of this [`Transaction`] instead of
        /// saving them.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__internal.html#ga73a5938ae4c3239ee11efa07eb22b882
        #[inline]
        pub fn abort(mut self) {
            // SAFETY: we take ownership of [`Transaction`], so it's safe to abort it.
            unsafe { sys::mdb_txn_abort(self.as_raw_ptr()) }
            self.is_discarded = true;
        }

        /// [`Commits`][0] all operations of this [`Transaction`] into the
        /// database.
        ///
        /// # Errors
        ///
        /// * Returns an [`Error`] if any call to LMDB API fails.
        ///
        /// [0]: http://www.lmdb.tech/doc/group__internal.html#ga846fbd6f46105617ac9f4d76476f6597
        #[inline]
        pub fn commit(mut self) -> Result<()> {
            // SAFETY: the ffi call is immediately followed by an error check
            let r = unsafe { sys::mdb_txn_commit(self.as_raw_ptr()) };
            if r != 0 {
                return Err(Error(r));
            }
            self.is_discarded = true;
            Ok(())
        }

        #[inline]
        #[must_use]
        pub(crate) fn as_raw_ptr(&self) -> *mut sys::MDB_txn {
            self.txn
        }
    }

    impl Drop for Transaction {
        #[inline]
        fn drop(&mut self) {
            ACTIVE_TXN.with(|cell| assert!(cell.replace(false)));
            if !self.is_discarded {
                // SAFETY: this [`Transaction`] will not be used after this call so it's safe to
                // abort it
                unsafe { sys::mdb_txn_abort(self.as_raw_ptr()) }
            }
        }
    }
}
