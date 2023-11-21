package controller

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

type cacheValue struct {
	// endpoints are the list of 'active' endpoints for an EndpointSlice and their TTL
	endpoints map[string]*time.Time
	// value is the value of the cache entry as set by the implementer of Trackable
	value interface{}
}

// ownerRefTracker is a map of hashrings for a given OwnerReference UUID
type ownerRefTracker map[ownerRefUID]*cacheValue

type tracker struct {
	trackable Trackable
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

func newTracker(ttl *time.Duration, logger *slog.Logger, trackable Trackable) *tracker {
	return &tracker{
		trackable: trackable,
		ttl:       ttl,
		state:     make(map[cacheKey]ownerRefTracker),
		now:       time.Now,
		logger:    logger,
	}
}

// GetEndpoints returns the list of active endpoints from the cachedValue.
func (cv cacheValue) GetEndpoints() []string {
	var endpoints []string
	for endpoint := range cv.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

// saveOrMerge saves or merges an EndpointSlice into the cache
func (t *tracker) saveOrMerge(eps *discoveryv1.EndpointSlice) error {
	key, err := t.generateCacheKey(eps)
	if err != nil {
		return fmt.Errorf("saveOrMerge failed to generate cache key: %w", err)
	}

	endpointState, err := t.parseEndpointsFromSlice(eps)
	if err != nil {
		return fmt.Errorf("save or Merge failed to parse endpoints from slice: %w", err)
	}

	subKey := t.toSubKey(eps)
	if t.saveInPlace(key, subKey) {
		t.setState(key, subKey, t.toCacheValue(eps, endpointState.activeEndpoints))
		return nil
	}

	// At this point we know the following is true:
	// 1. we have seen this EndpointSlice for this Service before
	// 2. we care about TTLs, and we don't want to act instantaneously on involuntary disruptions
	// 3. we are going to mutate the state in some form or another, and we need to lock
	t.mut.Lock()
	defer t.mut.Unlock()
	existingStoredState, ok := t.state[key][subKey]
	if !ok {
		// this should never happen
		return fmt.Errorf("saveOrMerge failed to find existing cached value for key %s and subKey %s", key, subKey)
	}

	// remove terminating Pods from the stored cacheValue
	for _, endpoint := range endpointState.terminatingEndpoints {
		t.logger.Info("evicting terminating endpoint from cache",
			"endpoint", endpoint, "key", key, "subKey", subKey)
		delete(existingStoredState.endpoints, endpoint)
	}

	now := t.now()
	for k, v := range existingStoredState.endpoints {
		if v.Before(now) {
			// check if the entry has been refreshed in this update
			if _, ok := endpointState.activeEndpoints[k]; !ok {
				t.logger.Info("evicting expired endpoint from hashring",
					"endpoint", k, "hashring", key, "subKey", subKey)
				delete(existingStoredState.endpoints, k)
			}
		}
	}

	// add/refresh our TTLs by merging back in our updates
	for k, v := range endpointState.activeEndpoints {
		existingStoredState.endpoints[k] = v
	}

	newCacheValue := t.toCacheValue(eps, existingStoredState.endpoints)
	t.state[key][subKey] = newCacheValue
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
	t.logger.Info("evicting endpointslice shard", "key", key, "shard", subKey)
	// remove this shard
	delete(state, subKey)
	// check if we have any shards left
	if len(state) == 0 {
		t.logger.Info("evicting key", "key", key)
		// delete the hashring entirely if not
		delete(t.state, key)
	}
	return true, nil
}

func (t *tracker) setState(key cacheKey, subKey ownerRefUID, value *cacheValue) {
	t.mut.Lock()
	defer t.mut.Unlock()
	if _, ok := t.state[key]; !ok {
		t.state[key] = make(map[ownerRefUID]*cacheValue)
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

type endpointsResult struct {
	activeEndpoints      map[string]*time.Time
	terminatingEndpoints []string
}

func (t *tracker) parseEndpointsFromSlice(eps *discoveryv1.EndpointSlice) (endpointsResult, error) {
	var (
		ttl    *time.Time
		result = endpointsResult{
			activeEndpoints: make(map[string]*time.Time),
		}
	)

	if t.ttl != nil {
		newTTL := t.now().Add(*t.ttl)
		ttl = &newTTL
	}

	for _, endpoint := range eps.Endpoints {
		createEndpointString := t.trackable.EndpointBuilder()
		ep, err := createEndpointString(eps, endpoint)
		if err != nil {
			t.logger.Warn("EndpointSlice endpoint has no hostname - skipping", "endpoint", endpoint.String())
			continue
		}

		if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating == true {
			// this is a voluntary disruption, so we should remove it from the hashring
			// it might be an indication of a scale down event or rolling update etc
			result.terminatingEndpoints = append(result.terminatingEndpoints, ep)
			continue
		}

		// we only care about ready endpoints in terms of adding nodes to the hashring
		if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready == true {
			result.activeEndpoints[ep] = ttl
		}
	}
	return result, nil
}

// toCacheValue converts an EndpointSlice and the active endpoints into a cacheValue.
func (t *tracker) toCacheValue(eps *discoveryv1.EndpointSlice, activeEndpoints map[string]*time.Time) *cacheValue {
	var endpoints []string
	for k, _ := range activeEndpoints {
		endpoints = append(endpoints, k)
	}

	value, err := t.trackable.SetValueFor(eps, endpoints)
	if err != nil {
		t.logger.Error("failed to set value for EndpointSlice", "name", eps.GetName(), "error", err.Error())
		return nil
	}

	return &cacheValue{
		endpoints: activeEndpoints,
		value:     value,
	}
}

func (t *tracker) generateCacheKey(eps *discoveryv1.EndpointSlice) (cacheKey, error) {
	key, ok := eps.GetLabels()[discoveryv1.LabelServiceName]
	if !ok || key == "" {
		return "", fmt.Errorf("EndpointSlice %s/%s does not have a %s label",
			eps.Namespace, eps.Name, discoveryv1.LabelServiceName)
	}

	return cacheKey(key), nil
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

// generate outputs the sorted hashrings that are currently in the cache.
// It does not look at TTLs and takes anything that is in the cache
// as the source of truth. It is read only and safe to call concurrently.
func (t *tracker) generate() (string, []metav1.OwnerReference, error) {
	t.mut.RLock()
	defer t.mut.RUnlock()
	var owners []metav1.OwnerReference

	var groupedValues [][]interface{}
	for _, ownerRefs := range t.state {
		var values []interface{}
		for nameAndUID, cachedValue := range ownerRefs {
			owners = append(owners, t.buildOwnerReference(nameAndUID))
			values = append(values, cachedValue.value)
		}
		groupedValues = append(groupedValues, values)
	}

	// sort the owners to avoid flapping on the ConfigMap
	sort.Slice(owners, func(i, j int) bool { return owners[i].Name < owners[j].Name })
	output, err := t.trackable.Generate(groupedValues)
	if err != nil {
		return "", nil, err
	}

	return output, owners, nil
}

func (t *tracker) buildOwnerReference(key ownerRefUID) metav1.OwnerReference {
	name, uid := t.fromSubKey(key)

	return metav1.OwnerReference{
		APIVersion: discoveryv1.SchemeGroupVersion.String(),
		Kind:       "EndpointSlice",
		Name:       name,
		UID:        uid,
	}
}
