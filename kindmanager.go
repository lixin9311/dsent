// RegisterKind registers a new kind with the given name.
// It checks if the kind is already registered and panics if it is.
package dsent

import "sync"

var registeredKinds = map[string]bool{}
var lock sync.Mutex

func RegisterKind(name string) {
	lock.Lock()
	defer lock.Unlock()
	if registeredKinds[name] {
		panic("kind already registered: " + name)
	}
	registeredKinds[name] = true
}
