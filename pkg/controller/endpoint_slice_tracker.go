package controller

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
)

// cacheKey is the cache key and the name of the hashring
// combined with the name of the headless Service which owns the EndpointSlice
// in the format <HashringName>/<Service.Name>
type cacheKey string

// ownerRefUID is the cache sub key and is made of a combination of
// the EndpointSlice OwnerReference name and the EndpointSlice UID
// in the format <EndpointSlice.Name>/<EndpointSlice.UID>
type ownerRefUID string

// hashring is a representation of Thanos hashring configuration
// which caches the endpoints and their expiry time
type hashring struct {
	tenants   []string
	endpoints map[string]*time.Time
}

// ownerRefTracker is a map of hashrings for a given OwnerReference UUID
type ownerRefTracker map[ownerRefUID]*hashring

type tracker struct {
	// ttl is the TTL for endpoints in the hashring
	// This is used to allow for involuntary disruptions to remain in the hashring
	ttl *time.Duration
	// mut is a mutex to protect the state
	mut sync.RWMutex
	// state is a map of Service name to a map of OwnerReference UUID to hashring
	state map[cacheKey]ownerRefTracker
	// now is a function that returns the current time
	// it is used to allow for testing
	now    func() time.Time
	logger *slog.Logger
}

func newTracker(ttl *time.Duration, logger *slog.Logger) *tracker {
	return &tracker{
		ttl:    ttl,
		state:  make(map[cacheKey]ownerRefTracker),
		now:    time.Now,
		logger: logger,
	}
}

// saveOrMerge saves or merges an EndpointSlice into the cache
func (t *tracker) saveOrMerge(eps *discoveryv1.EndpointSlice) error {
	key, err := t.generateCacheKey(eps)
	if err != nil {
		return fmt.Errorf("saveOrMerge failed to generate cache key: %w", err)
	}

	updatedState, terminatingPods := t.toHashring(eps)
	subKey := t.toSubKey(eps)

	if t.saveInPlace(key, subKey) {
		t.setState(key, subKey, updatedState)
		return nil
	}

	// At this point we know the following is true:
	// 1. we have seen this EndpointSlice for this Service before
	// 2. we care about TTLs and we don't want to act on involuntary disruptions
	// 3. we are going to mutate the state in some form or another so we need to lock
	t.mut.Lock()
	defer t.mut.Unlock()
	existingStoredState, ok := t.state[key][subKey]
	if !ok {
		// this should never happen
		return fmt.Errorf("saveOrMerge failed to find existing hashring for key %s and subKey %s", key, subKey)
	}

	// remove terminating Pods from the stored hashring
	for _, hostname := range terminatingPods {
		t.logger.Info("evicting terminating endpoint from hashring",
			"endpoint", hostname, "hashring", key, "subKey", subKey)
		delete(existingStoredState.endpoints, hostname)
	}

	now := t.now()
	for k, v := range existingStoredState.endpoints {
		if v.Before(now) {
			// check if the entry has been refreshed in this update
			if _, ok := updatedState.endpoints[k]; !ok {
				t.logger.Info("evicting expired endpoint from hashring",
					"endpoint", k, "hashring", key, "subKey", subKey)
				delete(existingStoredState.endpoints, k)
			}
		}
	}

	// add/refresh our TTLs by merging back in our updates
	for k, v := range updatedState.endpoints {
		existingStoredState.endpoints[k] = v
	}

	return nil
}

// evict evicts an EndpointSlice from the cache
func (t *tracker) evict(eps *discoveryv1.EndpointSlice) (bool, error) {
	key, err := t.generateCacheKey(eps)
	if err != nil {
		return false, fmt.Errorf("evict failed to generate cache key: %w", err)
	}

	t.mut.Lock()
	defer t.mut.Unlock()

	state, ok := t.state[key]
	if !ok {
		return false, nil
	}

	subKey := t.toSubKey(eps)
	_, ok = state[subKey]
	if !ok {
		return false, nil
	}
	t.logger.Info("evicting hashring shard", "hashring", key, "shard", subKey)
	// remove this shard
	delete(state, subKey)
	// check if we have any shards left
	if len(state) == 0 {
		t.logger.Info("evicting hashring", "hashring", key)
		// delete the hashring entirely if not
		delete(t.state, key)
	}
	return true, nil
}

func (t *tracker) deepCopyState() map[cacheKey]ownerRefTracker {
	t.mut.RLock()
	defer t.mut.RUnlock()

	newState := make(map[cacheKey]ownerRefTracker, len(t.state))

	for name, refTracker := range t.state {
		newOwnerRefTracker := make(ownerRefTracker)
		for subKey, hashringState := range refTracker {
			newHashring := &hashring{
				tenants:   append([]string(nil), hashringState.tenants...),
				endpoints: make(map[string]*time.Time, len(hashringState.endpoints)),
			}
			for endpoint, expiry := range hashringState.endpoints {
				var newExpiry *time.Time
				if expiry != nil {
					newExpiry = new(time.Time)
					*newExpiry = *expiry
				}
				newHashring.endpoints[endpoint] = newExpiry
			}
			newOwnerRefTracker[subKey] = newHashring
		}
		newState[name] = newOwnerRefTracker
	}

	return newState
}

func (t *tracker) setState(key cacheKey, subKey ownerRefUID, value *hashring) {
	t.mut.Lock()
	defer t.mut.Unlock()
	if _, ok := t.state[key]; !ok {
		t.state[key] = make(map[ownerRefUID]*hashring)
	}

	t.state[key][subKey] = value
}

// saveInPlace returns true if the state for this key/subKey should be saved in place
// This is true if the TTL is nil or if the state for this key/subKey does not exist
func (t *tracker) saveInPlace(key cacheKey, subKey ownerRefUID) bool {
	if t.ttl == nil {
		return true
	}
	return !t.hasStateForSubKey(key, subKey)
}

func (t *tracker) hasStateForSubKey(key cacheKey, subKey ownerRefUID) bool {
	t.mut.RLock()
	defer t.mut.RUnlock()
	if _, ok := t.state[key]; !ok {
		return false
	}
	if _, ok := t.state[key][subKey]; !ok {
		return false
	}
	return true
}

// toHashring converts an EndpointSlice into a hashring
// It adds all ready endpoints to the hashring and sets the TTL if provided
// It returns a list of terminating Pods to be considered for eviction
func (t *tracker) toHashring(eps *discoveryv1.EndpointSlice) (*hashring, []string) {
	var terminatingPods []string
	var endpoints = make(map[string]*time.Time)

	var ttl *time.Time
	if t.ttl != nil {
		newTTL := t.now().Add(*t.ttl)
		ttl = &newTTL
	}

	for _, endpoint := range eps.Endpoints {
		if endpoint.Hostname == nil {
			t.logger.Warn("EndpointSlice endpoint has no hostname - skipping", "endpoint", endpoint)
			continue
		}

		if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating == true {
			// this is a voluntary disruption, so we should remove it from the hashring
			// it might be an indication of a scale down event or rolling update etc
			terminatingPods = append(terminatingPods, *endpoint.Hostname)
			continue
		}

		// we only care about ready endpoints in terms of adding nodes to the hashring
		if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready == true {
			endpoints[*endpoint.Hostname] = ttl
		}
	}

	return &hashring{
		tenants:   t.getTenants(eps),
		endpoints: endpoints,
	}, terminatingPods
}

func (t *tracker) generateCacheKey(eps *discoveryv1.EndpointSlice) (cacheKey, error) {
	key, ok := eps.GetLabels()[discoveryv1.LabelServiceName]
	if !ok || key == "" {
		return "", fmt.Errorf("EndpointSlice %s/%s does not have a %s label",
			eps.Namespace, eps.Name, discoveryv1.LabelServiceName)
	}

	name, ok := t.getHashringName(eps)
	if !ok || name == "" {
		name = key
	}

	return cacheKey(fmt.Sprintf("%s/%s", name, key)), nil
}

// unwrapCacheKey returns the hashring name and the name of the service
func (t *tracker) unwrapCacheKey(key cacheKey) (string, string) {
	parts := strings.Split(string(key), "/")
	if len(parts) == 1 {
		return parts[0], parts[0]
	}
	return parts[0], parts[1]
}

func (t *tracker) getHashringName(eps *discoveryv1.EndpointSlice) (string, bool) {
	name, ok := eps.GetLabels()[HashringNameIdentifierLabel]
	return name, ok
}

func (t *tracker) getTenants(eps *discoveryv1.EndpointSlice) []string {
	tenant, ok := eps.GetLabels()[TenantIdentifierLabel]
	if !ok {
		return []string{}
	}
	return []string{tenant}
}

func (t *tracker) toSubKey(eps *discoveryv1.EndpointSlice) ownerRefUID {
	return ownerRefUID(fmt.Sprintf("%s/%s", eps.Name, eps.UID))
}

func (t *tracker) fromSubKey(subKey ownerRefUID) (name string, uid types.UID) {
	parts := strings.Split(string(subKey), "/")
	if len(parts) != 2 {
		// log error
		return name, uid
	}
	name = parts[0]
	uid = types.UID(parts[1])
	return name, uid
}
