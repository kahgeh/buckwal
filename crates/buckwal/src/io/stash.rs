//! Typed errors across the engine (D8). The IO wrappers run on the calling thread, so they
//! park the typed [`Error`] in a thread-local and hand the engine a `LimboError`; the caller
//! takes it back after the engine call returns `Err`.
use crate::error::Error;
use std::cell::RefCell;

thread_local! {
    static STASH: RefCell<Option<Error>> = const { RefCell::new(None) };
}

/// Keep `e` for this thread's caller and return the error the engine propagates.
pub(crate) fn stash(e: Error) -> turso_core::LimboError {
    let msg = format!("buckwal: {e}");
    STASH.with(|s| *s.borrow_mut() = Some(e));
    turso_core::LimboError::InternalError(msg)
}

/// The last stashed error on this thread, if any; clears it.
pub(crate) fn take() -> Option<Error> {
    STASH.with(|s| s.borrow_mut().take())
}

pub(crate) fn clear() {
    STASH.with(|s| *s.borrow_mut() = None);
}
