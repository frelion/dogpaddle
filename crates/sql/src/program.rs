use std::{fs, path::Path};

use datafusion_sql::sqlparser::{
    ast::Query,
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer},
};
use dogpaddle_flow::{Flow, FlowFactory};

use crate::{
    SqlError,
    assembly::{OUTPUT_CAPACITY_BYTES, scan_station_id},
    endpoint::{ScanEndpoint, SinkEndpoint, resolve_debezium_runtime},
    plan::{lower_query, plan},
    syntax,
};

const IDENTITY_DOMAIN: &[u8] = b"dogpaddle-sql/program-identity/v3";

/// One `INSERT INTO sink(...)` statement and its streaming query.
pub struct SqlProgram {
    sink: SinkEndpoint,
    query: Query,
    scans: Vec<ScanEndpoint>,
}

impl SqlProgram {
    /// Parses exactly one `DogPaddle` SQL program.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid SQL, syntax outside the supported query
    /// subset, any outer statement other than direct `INSERT INTO sink(...) Query`,
    /// or malformed endpoint parameters.
    pub fn parse(sql: &str) -> Result<Self, SqlError> {
        let (sink, query, scans) = syntax::parse(sql)?;
        Ok(Self { sink, query, scans })
    }

    /// Reads and parses one UTF-8 SQL file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or its contents do not
    /// form one valid `DogPaddle` SQL program.
    pub fn read(path: impl AsRef<Path>) -> Result<Self, SqlError> {
        let path = path.as_ref();
        let sql = fs::read_to_string(path).map_err(|source| SqlError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&sql)
    }

    /// Starts this program from new or existing durable state.
    ///
    /// # Errors
    ///
    /// Returns an error when endpoint parameters cannot be resolved, a new
    /// program cannot be planned or discovered, existing state is incomplete
    /// or belongs to another program, or the underlying Flow cannot start.
    pub fn start(&self, path: impl AsRef<Path>) -> Result<Flow, SqlError> {
        let supplied_path = path.as_ref();
        let program = self.resolved()?;
        let identity = program.identity()?;
        let runtime_bundle = program
            .scans
            .iter()
            .any(ScanEndpoint::needs_debezium)
            .then(resolve_debezium_runtime)
            .transpose()?;
        let (path, exists) = resolve_state_path(supplied_path)?;
        if exists {
            program.open_existing(&path, identity, runtime_bundle.as_deref())
        } else {
            program.build_new(&path, identity, runtime_bundle.as_deref())
        }
    }

    fn resolved(&self) -> Result<Self, SqlError> {
        Ok(Self {
            sink: self.sink.resolved()?,
            query: self.query.clone(),
            scans: self
                .scans
                .iter()
                .map(ScanEndpoint::resolved)
                .collect::<Result<_, _>>()?,
        })
    }

    fn build_new(
        &self,
        path: &Path,
        identity: [u8; 32],
        runtime_bundle: Option<&Path>,
    ) -> Result<Flow, SqlError> {
        let scans = self
            .scans
            .iter()
            .enumerate()
            .map(|(index, scan)| scan.build(&identity, index, path, runtime_bundle))
            .collect::<Result<Vec<_>, _>>()?;
        let logical_plan = plan(self.query.clone(), &scans)?;
        let mut factory = FlowFactory::new(path);
        factory.owner_identity(identity);
        let query = lower_query(&logical_plan, scans)?;
        let sink = self.sink.build(&identity, path)?;
        let factory = query.emit(factory, sink)?;
        factory.build().map_err(Into::into)
    }

    fn open_existing(
        &self,
        path: &Path,
        identity: [u8; 32],
        runtime_bundle: Option<&Path>,
    ) -> Result<Flow, SqlError> {
        let mut factory = FlowFactory::new(path);
        factory.owner_identity(identity);
        for (index, scan) in self.scans.iter().enumerate() {
            let station_id = scan_station_id(index);
            scan.install_open_runtime_resource(&mut factory, &station_id, runtime_bundle)?;
        }
        self.sink.install_open_runtime_resource(&mut factory)?;
        factory.open().map_err(Into::into)
    }

    fn identity(&self) -> Result<[u8; 32], SqlError> {
        let mut encoded = Vec::new();
        write_identity_bytes(&mut encoded, IDENTITY_DOMAIN);
        write_identity_bytes(&mut encoded, canonical_query(&self.query).as_bytes());
        encoded.extend_from_slice(&OUTPUT_CAPACITY_BYTES.to_be_bytes());
        let scan_count = u64::try_from(self.scans.len()).expect("a Vec length fits in u64");
        encoded.extend_from_slice(&scan_count.to_be_bytes());
        for scan in &self.scans {
            scan.write_identity(&mut encoded)?;
        }
        self.sink.write_identity(&mut encoded)?;
        Ok(*blake3::hash(&encoded).as_bytes())
    }
}

fn canonical_query(query: &Query) -> String {
    let rendered = query.to_string();
    let mut tokenizer = Tokenizer::new(&GenericDialect, &rendered);
    let mut tokens = tokenizer
        .tokenize()
        .expect("a rendered SQL AST must tokenize again");
    for token in &mut tokens {
        if let Token::Word(word) = token
            && word.quote_style.is_none()
        {
            word.value.make_ascii_lowercase();
        }
    }
    tokens.into_iter().map(|token| token.to_string()).collect()
}

fn resolve_state_path(supplied: &Path) -> Result<(std::path::PathBuf, bool), SqlError> {
    let absolute = std::path::absolute(supplied).map_err(|source| SqlError::StatePath {
        path: supplied.to_path_buf(),
        source,
    })?;
    let exists = absolute
        .try_exists()
        .map_err(|source| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source,
        })?;
    let path = if exists {
        fs::canonicalize(&absolute).map_err(|source| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source,
        })?
    } else {
        let parent = absolute.parent().ok_or_else(|| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "state path has no parent directory",
            ),
        })?;
        fs::create_dir_all(parent).map_err(|source| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source,
        })?;
        let parent = fs::canonicalize(parent).map_err(|source| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source,
        })?;
        let name = absolute.file_name().ok_or_else(|| SqlError::StatePath {
            path: supplied.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "state path has no final component",
            ),
        })?;
        parent.join(name)
    };
    if path.to_str().is_none() {
        return Err(SqlError::StatePath {
            path: supplied.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "state path must be valid UTF-8",
            ),
        });
    }
    Ok((path, exists))
}

pub(crate) fn write_identity_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    let length = u64::try_from(value.len()).expect("a byte slice length fits in u64");
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(sql: &str) -> [u8; 32] {
        SqlProgram::parse(sql).unwrap().identity().unwrap()
    }

    #[test]
    fn missing_state_path_canonicalizes_its_parent() {
        let root = tempfile::tempdir().unwrap();
        let supplied = root.path().join("nested/../state");

        let (resolved, exists) = resolve_state_path(&supplied).unwrap();

        assert!(!exists);
        assert_eq!(
            resolved,
            fs::canonicalize(root.path()).unwrap().join("state")
        );
    }

    #[test]
    fn program_identity_has_one_stable_canonical_encoding() {
        let identity = identity(
            "INSERT INTO discard() SELECT value FROM sequence(start => 7) WHERE value > 10",
        );
        assert_eq!(
            blake3::Hash::from(identity).to_hex().as_str(),
            "85ed0ec942fe61e0aa385e1404755f61eacb813b1a641490e7852f45867ecddd"
        );
    }

    #[test]
    fn formatting_and_endpoint_argument_order_do_not_change_identity() {
        let first = identity(
            "INSERT INTO sqlite(path => '/tmp/result.sqlite', table => 'result') \
             SELECT value FROM sequence(start => 7)",
        );
        let second = identity(
            "-- formatting is not program identity\n\
             INSERT INTO sqlite(table => 'result', path => '/tmp/result.sqlite')\n\
             SELECT value\nFROM sequence(start => 7);",
        );
        assert_eq!(first, second);
    }

    #[test]
    fn asof_direction_is_semantic_but_formatting_is_not_identity() {
        let backward = identity(
            "INSERT INTO discard() \
             SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan \
             ASOF JOIN sequence(start => 1) AS right_scan \
             MATCH_CONDITION (left_scan.value >= right_scan.value)",
        );
        let reformatted = identity(
            "INSERT INTO discard()\n\
             SELECT LEFT_SCAN.VALUE\n\
             FROM sequence(start => 0) AS left_scan\n\
             ASOF JOIN sequence(start => 1) AS right_scan\n\
             MATCH_CONDITION(left_scan.value>=right_scan.value);",
        );
        let forward = identity(
            "INSERT INTO discard() \
             SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan \
             ASOF JOIN sequence(start => 1) AS right_scan \
             MATCH_CONDITION (left_scan.value <= right_scan.value)",
        );

        assert_eq!(backward, reformatted);
        assert_ne!(backward, forward);
    }

    #[test]
    fn unquoted_identifier_case_is_canonical_but_string_case_is_semantic() {
        let lower = identity(
            "INSERT INTO discard() SELECT value, 'kept' AS label FROM sequence(start => 7)",
        );
        let upper = identity(
            "insert into DISCARD() select VALUE, 'kept' as LABEL from SEQUENCE(start => 7)",
        );
        let changed_literal = identity(
            "INSERT INTO discard() SELECT value, 'KEPT' AS label FROM sequence(start => 7)",
        );
        assert_eq!(lower, upper);
        assert_ne!(lower, changed_literal);
    }

    #[test]
    fn connection_runtime_fields_do_not_change_identity() {
        let first = identity(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://alice:first@127.0.0.1:5432/app',\
                table => 'public.orders', publication => 'orders_publication'\
            )",
        );
        let second = identity(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                publication => 'orders_publication', table => 'public.orders',\
                connection => 'postgres://bob:second@127.0.0.2:6432/app',\
                bootstrap_spool_bytes => 1073741824\
            )",
        );
        assert_eq!(first, second);

        let changed_database = identity(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgres://bob:second@127.0.0.2:6432/other',\
                table => 'public.orders', publication => 'orders_publication'\
            )",
        );
        assert_ne!(first, changed_database);

        for endpoint in ["doris", "clickhouse"] {
            let first = identity(&format!(
                "INSERT INTO {endpoint}(\
                    connection => '{endpoint}://alice:first@127.0.0.1:8123/app', \
                    table => 'app.orders'\
                 ) SELECT value FROM sequence(start => 0)"
            ));
            let second = identity(&format!(
                "INSERT INTO {endpoint}(\
                    connection => '{endpoint}://bob:second@127.0.0.2:9123/app', \
                    table => 'app.orders'\
                 ) SELECT value FROM sequence(start => 0)"
            ));
            let changed_database = identity(&format!(
                "INSERT INTO {endpoint}(\
                    connection => '{endpoint}://bob:second@127.0.0.2:9123/other', \
                    table => 'other.orders'\
                 ) SELECT value FROM sequence(start => 0)"
            ));
            assert_eq!(first, second);
            assert_ne!(first, changed_database);
        }
    }

    #[test]
    fn cdc_runtime_tuning_does_not_change_identity() {
        let postgres = identity(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders', publication => 'orders_publication'\
            )",
        );
        let tuned_postgres = identity(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders', publication => 'orders_publication', \
                connect_timeout_ms => 7000, query_timeout_ms => 8000, \
                retry_limit => 9, retry_max_delay_ms => 11000, \
                heartbeat_interval_ms => 2000, snapshot_fetch_size => 4096\
            )",
        );
        assert_eq!(postgres, tuned_postgres);

        let mysql = identity(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.orders'\
            )",
        );
        let tuned_mysql = identity(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.orders', connect_timeout_ms => 7000, \
                query_timeout_ms => 8000, retry_limit => 9, \
                retry_max_delay_ms => 11000, heartbeat_interval_ms => 2000, \
                snapshot_fetch_size => 4096\
            )",
        );
        assert_eq!(mysql, tuned_mysql);
    }
}
