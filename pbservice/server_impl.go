package pbservice

/* Notes:

passing:
TestBasicFail
TestFailPut
TestConcurrentSameUnreliable
TestPartition1
TestPartition2
TestConcurrentSame
TestConcurrentSameAppend
TestRepeatedCrash
TestRepeatedCrashUnreliable

failing:
TestAtMostOnce
TestRepeatedCrashUnreliable
*/

// additions to PBServer state.
//
// GIOVANNI PART
type PBServerImpl struct {
	kvMap                map[string]string
	Viewnum              uint
	Primary              string
	Backup               string
	LastRequestProcessed map[int64]int64 // map of clientID to last requestID processed
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	pb.impl = PBServerImpl{
		kvMap:                make(map[string]string),
		Viewnum:              0,
		Primary:              "",
		Backup:               "",
		LastRequestProcessed: make(map[int64]int64),
	}

}

// server Get() RPC handler.
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// log.Printf("[%s] Get RPC received with args: %+v\n", pb.me, args)

	// Only primary can process Get() requests
	// But the issue is that this server may think it is the primary, but it is not anymore in the real view
	// So we need to check with the viewservice to see if we are still the primary

	// ping viewservice to find current view
	realView, err := pb.vs.Ping(pb.impl.Viewnum)

	if err != nil {
		return err
	}

	// if this server is the primary in the real view, then we can process the request
	if pb.me != realView.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	// Check if the request is a duplicate and handle it
	lastReq, exists := pb.impl.LastRequestProcessed[args.Impl.ClientID]
	if exists && lastReq >= args.Impl.RequestID {
		reply.Value = pb.impl.kvMap[args.Key] // Replying with value even for duplicate requests
		reply.Err = OK
		return nil
	}

	// Handle the actual Get() request
	val, exists := pb.impl.kvMap[args.Key]
	if exists {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	// Update the LastRequestProcessed map since the request has been processed
	pb.impl.LastRequestProcessed[args.Impl.ClientID] = args.Impl.RequestID

	return nil
}

// ------------------------------------------------------------------------------

// GRACE PART
//
// server PutAppend() RPC handler.
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// log.Printf("[%s] PutAppend RPC received with args: %+v\n", pb.me, args)
	// log.Printf("[%s} current primary is %s\n", pb.me, pb.impl.Primary)

	// only the primary should be able to handle PutAppend requests
	if pb.me != pb.impl.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	// don't serve duplicate requests to ensure at most once semantics
	//if the request is marked as processed, then the write already went through and we need not serve it again
	if pb.impl.LastRequestProcessed[args.Impl.ClientID] == args.Impl.RequestID {
		// reply.Err = "Duplicate Request"
		// for whatever reason, the originval OK reply we sent may not have reached the client, so send it again?
		reply.Err = OK
		return nil
	}

	if pb.impl.Backup != "" {

		// forward the operation to the backup first, before making local changes

		fwdArgs := &ForwardPutArgs{
			ClientID:  args.Impl.ClientID,
			RequestID: args.Impl.RequestID,
			Operation: args.Impl.Operation,
			Key:       args.Key,
			Value:     args.Value,
		}
		var fpReply ForwardPutReply
		call(pb.impl.Backup, "PBServer.ForwardPut", fwdArgs, &fpReply)

		//check if the forward put was successful
		if fpReply.Err == OK {

			//only make the local update if the backup was successfuly updated
			curr, exists := pb.impl.kvMap[args.Key]

			// if key does not exist, append should use an empty string for previous value
			if args.Impl.Operation == "Put" {
				pb.impl.kvMap[args.Key] = args.Value
			} else if args.Impl.Operation == "Append" {
				if exists {
					pb.impl.kvMap[args.Key] = curr + args.Value
				} else {
					pb.impl.kvMap[args.Key] = args.Value
				}
			}

			pb.impl.LastRequestProcessed[args.Impl.ClientID] = args.Impl.RequestID

			// we should only indicate to the client that the request was successful if the backup was also successfuly updated
			reply.Err = OK
			return nil

		} else { //if the backup was not successfuly updated, we should not update the local state
			reply.Err = fpReply.Err
			return nil
		}
	} // END IF

	// if we got to this point it means this server is the primary with NO BACKUP
	// so don't need to worry about forwarding the put
	// write the new value locally:
	curr, exists := pb.impl.kvMap[args.Key]

	// if key does not exist, append should use an empty string for previous value
	if args.Impl.Operation == "Put" {
		pb.impl.kvMap[args.Key] = args.Value
	} else if args.Impl.Operation == "Append" {
		if exists {
			pb.impl.kvMap[args.Key] = curr + args.Value
		} else {
			pb.impl.kvMap[args.Key] = args.Value
		}
	}

	// request served and return
	pb.impl.LastRequestProcessed[args.Impl.ClientID] = args.Impl.RequestID

	reply.Err = OK
	return nil

} // END PUTAPPEND

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// log.Printf("[%s] tick() pulse check 1 my current viewnum is %d\n", pb.me, pb.impl.Viewnum)

	// ping viewservice to find current view
	realView, err := pb.vs.Ping(pb.impl.Viewnum) // since "vs" is a viewservice CLERK, we can use the function Ping() which will in turn do the RPC correctly

	if err != nil {
		return
	}

	if realView.Primary == pb.me && pb.impl.Viewnum < realView.Viewnum { // if this server is the primary in the new view, initiate view transition

		//before transitioning to new view, sync up with backup (of the new view bc that is the one we will be transitioning to)
		if realView.Backup != "" {

			// log.Printf("[%s] tick() pulse check 2 bootstrap with backup %s\n", pb.me, realView.Backup)

			//send the kv data to the backup
			args := &ForwardDatabaseArgs{
				Data: pb.impl.kvMap,
			}
			var fdbReply ForwardDatabaseReply
			call(realView.Backup, "PBServer.ForwardDatabase", args, &fdbReply)

			// log.Printf("[%s] tick() pulse check 3 result of db forward %s\n", pb.me, fdbReply.Err)

			//check if the forward database was successful. only then do we update the state
			if fdbReply.Err == OK {
				//update the viewnum
				pb.impl.Viewnum = realView.Viewnum
				//update the primary and backup
				pb.impl.Primary = realView.Primary
				pb.impl.Backup = realView.Backup
				//ACK the new view
				pb.vs.Ping(pb.impl.Viewnum)
			} else {
				// ping with old view to indicate that view transition did not take place YET (NO ACK)
				pb.vs.Ping(pb.impl.Viewnum)
			}

		} else { // if there is no backup, then we can just transition to the new view

			//update the viewnum
			pb.impl.Viewnum = realView.Viewnum
			//update the primary and backup
			pb.impl.Primary = realView.Primary
			pb.impl.Backup = realView.Backup
			//ACK the new view
			pb.vs.Ping(pb.impl.Viewnum)

		}

	} else if realView.Primary == pb.me && pb.impl.Viewnum >= realView.Viewnum { // this server is primary, but there is no new view to transition to

		// we still have to sync database with the backup if there is one
		if realView.Backup != "" {

			//send the kv data to the backup
			args := &ForwardDatabaseArgs{
				Data: pb.impl.kvMap,
			}
			var fdbReply ForwardDatabaseReply
			call(realView.Backup, "PBServer.ForwardDatabase", args, &fdbReply)

			//check if the forward database was successful
			if fdbReply.Err == OK {
				//what to do here?
			}

		}

		//ping again because why not????
		pb.vs.Ping(pb.impl.Viewnum)

	} else if realView.Primary != pb.me && pb.impl.Viewnum < realView.Viewnum { // otherwise this server is either an idle server or a backup server in the new view
		// backup or idle server has no responsibilities other than to stay up to date on the new view
		pb.impl.Viewnum = realView.Viewnum
		pb.impl.Primary = realView.Primary
		pb.impl.Backup = realView.Backup

		//let the viewservice know of our change to the view state
		pb.vs.Ping(pb.impl.Viewnum)
	}
} //END TICK

//
// add RPC handlers for any new RPCs that you include in your design.
//

// RPC Handler for the ForwardDatabase RPC.
func (pb *PBServer) ForwardDatabase(args *ForwardDatabaseArgs, reply *ForwardDatabaseReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Make sure the server recognizes it's a backup. It's a precautionary step.
	if pb.me != pb.impl.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	// log.Printf("[%s] ForwardDatabase RPC received with args: %+v\n", pb.me, args)

	// Replace the backup's database with the incoming data from the primary.
	pb.impl.kvMap = args.Data

	// Acknowledge the receipt of the database.
	reply.Err = OK

	return nil
}

// RPC Handler for the ForwardPut RPC.
func (pb *PBServer) ForwardPut(args *ForwardPutArgs, reply *ForwardPutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Make sure the server recognizes it's a backup. It's a precautionary step.
	if pb.me != pb.impl.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	// don't serve duplicate requests to ensure at most once semantics
	if pb.impl.LastRequestProcessed[args.ClientID] == args.RequestID {
		//reply.Err = "Duplicate Request"
		reply.Err = OK
		return nil
	}

	curr, exists := pb.impl.kvMap[args.Key]

	// if key does not exist, append should use an empty string for previous value
	if args.Operation == "Put" {
		pb.impl.kvMap[args.Key] = args.Value
	} else if args.Operation == "Append" {
		if exists {
			pb.impl.kvMap[args.Key] = curr + args.Value
		} else {
			pb.impl.kvMap[args.Key] = args.Value
		}
	}

	pb.impl.LastRequestProcessed[args.ClientID] = args.RequestID

	//acknowledge receipt of the put
	reply.Err = OK

	return nil
}
