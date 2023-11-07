package viewservice

import (
	// "fmt"

	"sync"
	"time"
)

// the state of each key-value server, whether primary or backup or idle
type serverState struct {
	lastPing time.Time // last time we received a ping from it
	viewNum  uint      // the view number it is on
}

// additions to ViewServer state.
type ViewServerImpl struct {
	mu           sync.Mutex
	currentView  View
	servers      map[string]*serverState // map of the key-value server -> it's state
	acknowledged bool                    // whether the primary has acknowledged the current view
}

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	// initialize the state of the view server, starting with view 0 and no key-value servers
	vs.impl = ViewServerImpl{
		currentView:  View{Viewnum: 0},
		servers:      make(map[string]*serverState),
		acknowledged: true,
	}
}

// server Ping() RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.impl.mu.Lock()
	defer vs.impl.mu.Unlock()

	//track whether we have incremented the view yet to ensure it only happens once in a ping
	incrementedView := false

	// the key-value server that sent this ping
	server := args.Me
	// state of this key-value server
	state, exists := vs.impl.servers[server]

	// fmt.Printf("Received ping from %s", server)
	if exists {
		// fmt.Printf(" with state, last ping %s and view number %d \n", state.lastPing.String(), state.viewNum)
	}

	//if the server isn't tracked, make a state for it and add it to the map of servers
	if !exists {
		state = &serverState{}
		vs.impl.servers[server] = state
		// fmt.Print(" with no state, so making new one \n")
	}

	// set the new ping time and view number
	state.lastPing = time.Now()
	state.viewNum = args.Viewnum

	// set the state of the server in impl.servers with the updated values
	vs.impl.servers[server] = state

	// if the viewNum of the key-value server is 0, it restarted or is unititialized
	if args.Viewnum == 0 {
		//add it again to the server list with the new state (viewNum of 0, but will be updated)
		vs.impl.servers[server] = state

		//this is the case for the very first ping from the very first server (ACK is initialized to true)
		if len(vs.impl.servers) == 1 && vs.impl.acknowledged {
			vs.impl.currentView.Primary = server
			if !incrementedView {
				vs.impl.currentView.Viewnum++
				incrementedView = true
			}
			vs.impl.acknowledged = false
		}
	}

	// If this ping is the primary server acknowledging the current view
	if server == vs.impl.currentView.Primary && args.Viewnum == vs.impl.currentView.Viewnum {
		vs.impl.acknowledged = true
	}

	// only progress the view if the current view is acknowledged
	if vs.impl.acknowledged {
		curr := vs.impl.currentView.Primary
		// proper way to use the map
		if primary, existed := vs.impl.servers[curr]; existed { // body only runs if the exist is true
			prevPing := primary.lastPing

			// If primary is dead or restarted
			if time.Since(prevPing) > DeadPings*PingInterval || primary.viewNum == 0 {

				// fmt.Print("pulse check 1\n")

				//only promote backup to primary if backup is initialized (viewNum > 0)
				if vs.impl.currentView.Backup != "" {
					if vs.impl.servers[vs.impl.currentView.Backup].viewNum > 0 {
						vs.impl.currentView.Primary = vs.impl.currentView.Backup
						vs.impl.currentView.Backup = ""
						if !incrementedView {
							vs.impl.currentView.Viewnum++
							incrementedView = true
						}
						vs.impl.acknowledged = false
					}
				}
			}

			// If backup is dead
			if vs.impl.currentView.Backup != "" && time.Since(vs.impl.servers[vs.impl.currentView.Backup].lastPing) > DeadPings*PingInterval {
				// fmt.Print("pulse check 2\n")
				vs.impl.currentView.Backup = ""
				if !incrementedView {
					vs.impl.currentView.Viewnum++
					incrementedView = true
				}
				vs.impl.acknowledged = false

				// If there's no backup, select one
			} else if vs.impl.currentView.Backup == "" {
				for s, sState := range vs.impl.servers {

					// fmt.Printf("Checking server %s with the state time %s viewNum %d \n", s, sState.lastPing.String(), sState.viewNum)

					if s != vs.impl.currentView.Primary && time.Since(sState.lastPing) <= DeadPings*PingInterval {
						vs.impl.currentView.Backup = s
						if !incrementedView {
							vs.impl.currentView.Viewnum++
							incrementedView = true
						}
						vs.impl.acknowledged = false
						break
					}
				}
			}

		}

	}

	// fmt.Printf("[ping] Current view is %d with primary %s and backup %s\n", vs.impl.currentView.Viewnum, vs.impl.currentView.Primary, vs.impl.currentView.Backup)
	reply.View = vs.impl.currentView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.impl.currentView

	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// log.Printf("[viewservice] pulse check 1. the current viewnum is %d and the primary and backups are %s and %s \n", vs.impl.currentView.Viewnum, vs.impl.currentView.Primary, vs.impl.currentView.Backup)

	//track whether we have incremented the view yet to ensure it only happens once in a tick
	incrementedView := false

	curr := vs.impl.currentView.Primary
	if primary, existed := vs.impl.servers[curr]; existed {

		next := vs.impl.currentView.Backup
		if backup, exists := vs.impl.servers[next]; exists {

			if vs.impl.acknowledged {
				//for all the following test cases, we make sure server is not primary or backup (so "server" is an idle server)
				//then we check the state of primary and backup to see how we should update the view

				// log.Printf("pulse check 2\n")

				//if ONLY the primary failed, the backup should become the primary and an idle server (if any) should become the backup
				if time.Since(primary.lastPing) > DeadPings*PingInterval &&
					time.Since(backup.lastPing) <= DeadPings*PingInterval {

					idleServer := ""

					for server, serverState := range vs.impl.servers {
						//we need to make sure the server is not the primary or backup and that it is alive, otherwise it is not a valid idle server
						if server != vs.impl.currentView.Primary && server != vs.impl.currentView.Backup && time.Since(serverState.lastPing) <= DeadPings*PingInterval {
							idleServer = server
							break
						}
					}

					// log.Printf("backup and primary are %s, %s\n", vs.impl.currentView.Backup, vs.impl.currentView.Primary)
					// log.Printf("backup's viewnum is %d\n", backup.viewNum)

					//only promote backup to primary if backup is initialized (viewNum > 0)
					if backup.viewNum > 0 {
						vs.impl.currentView.Primary = vs.impl.currentView.Backup
						vs.impl.currentView.Backup = idleServer

						if !incrementedView {
							vs.impl.currentView.Viewnum++
							incrementedView = true
						}
						vs.impl.acknowledged = false
					}
				}

				//if ONLY the backup failed but the primary did not, we remove the backup and an idle server (if any) should become the backup
				if time.Since(backup.lastPing) > DeadPings*PingInterval &&
					time.Since(primary.lastPing) <= DeadPings*PingInterval {

					idleServer := ""

					for server, serverState := range vs.impl.servers {
						//we need to make sure the server is not the primary or backup and that it is alive, otherwise it is not a valid idle server
						if server != vs.impl.currentView.Primary && server != vs.impl.currentView.Backup && time.Since(serverState.lastPing) <= DeadPings*PingInterval {
							idleServer = server
							break
						}
					}

					vs.impl.currentView.Backup = idleServer

					if !incrementedView {
						vs.impl.currentView.Viewnum++
						incrementedView = true
					}
					vs.impl.acknowledged = false
				}

				//if both the primary and backup failed, an idle server (if any) should become the primary and the backup should be empty
				if time.Since(backup.lastPing) > DeadPings*PingInterval &&
					time.Since(primary.lastPing) > DeadPings*PingInterval {

					idleServer := ""

					for server, serverState := range vs.impl.servers {
						//we need to make sure the server is not the primary or backup and that it is alive, otherwise it is not a valid idle server
						//here we also need to verify the idle server is initialized (viewNum > 0)
						if server != vs.impl.currentView.Primary && server != vs.impl.currentView.Backup && time.Since(serverState.lastPing) <= DeadPings*PingInterval && serverState.viewNum > 0 {
							idleServer = server
							break
						}
					}

					vs.impl.currentView.Primary = idleServer
					vs.impl.currentView.Backup = ""

					if !incrementedView {
						vs.impl.currentView.Viewnum++
						incrementedView = true
					}
					vs.impl.acknowledged = false
				}
			}
		}

	}

	// fmt.Printf("[tick] Current view is %d with primary %s and backup %s\n", vs.impl.currentView.Viewnum, vs.impl.currentView.Primary, vs.impl.currentView.Backup)

}
