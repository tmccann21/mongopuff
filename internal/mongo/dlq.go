package mongo

// This file will contain the real DLQ writer implementation
// that satisfies the DLQWriter interface.
//
// Implementation will use the official MongoDB Go driver to:
// - Insert DLQEntry documents into the _mongopuff_dlq collection
