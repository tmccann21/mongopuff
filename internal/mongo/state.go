package mongo

// This file will contain the real MongoDB state store implementation
// that satisfies the StateStore interface.
//
// Implementation will use the official MongoDB Go driver to:
// - Read/write _mongopuff collection documents (resume tokens, backfill cursors)
// - Read/write the _global document for service-level config
// - Update lastFlushTime after successful batch writes
