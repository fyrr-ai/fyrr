//! Bound independent store operations and drain admitted work before releasing authority.
use crate::Result;
use futures::{StreamExt, stream::FuturesUnordered};
use std::future::Future;

pub(crate) const CONCURRENCY: usize = 8;

pub(crate) async fn bounded<I, F, Fut, T>(items: I, mut operation: F) -> Result<Vec<T>>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut items = items.into_iter();
    let mut pending = FuturesUnordered::new();
    for item in items.by_ref().take(CONCURRENCY) {
        pending.push(operation(item));
    }
    let mut values = Vec::new();
    let mut failure = None;
    while let Some(result) = pending.next().await {
        match result {
            Ok(value) => values.push(value),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
        // After the first error admit nothing else, but join every admitted
        // mutation. Returning early would release the enclosing lease too soon.
        if failure.is_none()
            && let Some(item) = items.next()
        {
            pending.push(operation(item));
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(values),
    }
}
