CREATE TABLE mt_nodes (
  mt_id INTEGER,
  key BLOB,
  type INTEGER NOT NULL,
  child_l BLOB,
  child_r BLOB,
  entry BLOB,
  created_at INTEGER,
  deleted_at INTEGER,
  PRIMARY KEY(mt_id, key)
);

CREATE TABLE mt_roots (
  mt_id INTEGER PRIMARY KEY,
  key BLOB,
  created_at INTEGER,
  deleted_at INTEGER
);
