use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;

/// Supplies untrusted configuration values to the parser.
pub trait ConfigSource {
    /// Returns the configured OS-native value for `key`, if present.
    fn get(&self, key: &str) -> Option<OsString>;
}

/// Reads configuration from the current process environment.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvSource;

impl ConfigSource for EnvSource {
    fn get(&self, key: &str) -> Option<OsString> {
        std::env::var_os(key)
    }
}

/// An immutable map-backed source intended for deterministic tests.
#[derive(Clone, Default)]
pub struct MapSource {
    values: BTreeMap<OsString, OsString>,
}

impl fmt::Debug for MapSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MapSource")
            .field("keys", &self.values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConfigSource for MapSource {
    fn get(&self, key: &str) -> Option<OsString> {
        self.values.get(OsStr::new(key)).cloned()
    }
}

impl<K, V, const N: usize> From<[(K, V); N]> for MapSource
where
    K: Into<OsString>,
    V: Into<OsString>,
{
    fn from(values: [(K, V); N]) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl<K, V> FromIterator<(K, V)> for MapSource
where
    K: Into<OsString>,
    V: Into<OsString>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            values: iter
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}
