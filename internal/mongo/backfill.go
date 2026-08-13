package mongo

// This file will contain the real MongoDB backfill scanner implementation
// that satisfies the BackfillScanner interface.
//
// Implementation will use the official MongoDB Go driver to:
// - Issue db.runCommand({ping:1}) and capture operationTime
// - Query documents with _id > cursor, sorted by _id, limited by page size
// - Return raw documents for transformation
