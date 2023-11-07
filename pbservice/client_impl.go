package pbservice

import (
	"time"
)

// ClerkImpl contains metadata about the client and the view of the distributed system.
type ClerkImpl struct {
	clientID  int64  // Unique identifier for the client. This helps differentiate requests from different clients.
	requestID int64  // Identifier for each request made by the client. This aids in ensuring at-most-once semantics.
	primary   string // The current primary server's address known to the client.
	viewnum   uint   // The current view number known to the client, indicating the configuration version.
}

// initImpl initializes the ClerkImpl, setting a unique clientID and resetting other values.
func (ck *Clerk) initImpl() {
	// Initialize the primary by fetching from the viewservice.
	ck.impl.primary = ck.vs.Primary()
	// Get the full view to initialize the viewnum.
	view, ok := ck.vs.Get()
	if ok {
		ck.impl.viewnum = view.Viewnum
	} else {
		ck.impl.viewnum = 0
	}
	ck.impl.clientID = nrand() // Assign a unique ID to this client.
	ck.impl.requestID = 1      // Initialize the request counter.
}

// fetchPrimary queries the viewservice to get the latest primary server's address and view number.
func (ck *Clerk) fetchPrimary() {
	// Directly call the Primary() and Get() methods on the viewservice's Clerk.
	ck.impl.primary = ck.vs.Primary()
	view, ok := ck.vs.Get()
	if ok {
		ck.impl.viewnum = view.Viewnum
	} else {
		ck.impl.viewnum = 0
	}
}

// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until either the
// primary replies with the value or the primary
// says the key doesn't exist, i.e., has never been Put().

// Get fetches the value associated with the given key from the primary server.
// It keeps trying until it succeeds or the primary indicates the key doesn't exist.
func (ck *Clerk) Get(key string) string {
	for {
		// If the client doesn't know the current primary, fetch it from the viewservice.
		if ck.impl.primary == "" {
			ck.fetchPrimary()
		}

		// keep note of current primary
		currPrimary := ck.impl.primary

		// Prepare the GetArgs with necessary metadata.
		args := GetArgs{
			Key: key,
			Impl: GetArgsImpl{
				ClientID:  ck.impl.clientID,
				RequestID: nrand(),
			},
		}

		var reply GetReply
		// Send a Get RPC to the known primary.
		ok := call(ck.impl.primary, "PBServer.Get", &args, &reply)

		// GRACES CHANGES TO ACCOUNT FOR OLD PRIMARY TRYING TO ISSUE GET()
		// Check if the primary has changed since the last fetch.
		if ck.impl.primary != "" && ck.impl.primary != currPrimary {
			// Primary has changed, clear the knowledge of the old primary and retry.
			ck.impl.primary = ""
			continue // Retry the request with the new primary.
		}

		// If RPC was successful and the primary returned a valid response, increment the request counter and return.
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.impl.requestID++
			return reply.Value //if ErrNoKey, the reply.Value is empty string
		} else if !ok || reply.Err == ErrWrongServer {
			// If there was an issue or the primary has changed, clear the known primary.
			ck.impl.primary = ""
		}

		// Introduce a short delay before retrying.
		time.Sleep(100 * time.Millisecond)
	}
}

// PutAppend sends a Put or Append RPC to the primary server.
// It keeps trying until the operation succeeds.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		// If the client doesn't know the current primary, fetch it from the viewservice.
		if ck.impl.primary == "" {
			ck.fetchPrimary()
		}

		// Prepare the PutAppendArgs with necessary metadata and operation details.
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Impl: PutAppendArgsImpl{
				ClientID:  ck.impl.clientID,
				RequestID: nrand(),
				Operation: op,
			},
		}

		var reply PutAppendReply
		// Send a Put or Append RPC to the known primary.
		ok := call(ck.impl.primary, "PBServer.PutAppend", &args, &reply)

		// If RPC was successful and the operation was completed by the primary, increment the request counter and return.
		if ok && reply.Err == OK {
			ck.impl.requestID++
			return
		} else if !ok || reply.Err == ErrWrongServer {
			// If there was an issue or the primary has changed, clear the known primary.
			ck.impl.primary = ""
		}

		// Introduce a short delay before retrying.
		time.Sleep(100 * time.Millisecond)
	}
}
