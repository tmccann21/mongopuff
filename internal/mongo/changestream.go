package mongo

// This file will contain the real MongoDB change stream implementation
// that satisfies the ChangeStreamIterator interface.
//
// Implementation will use the official MongoDB Go driver to:
// - Open a change stream on a specific collection
// - Resume from a stored resume token
// - Parse change events into ChangeEvent structs
// - Handle invalidate events by returning a fatal error
