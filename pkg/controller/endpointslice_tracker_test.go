package controller

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func TestSaveOrMerge(t *testing.T) {
	// build some known times for testing against
	now := time.Now()
	future := time.Now().Add(time.Hour)
	oneHourTTL := time.Hour
	expectOneHour := now.Add(oneHourTTL)
	oneNanoSecondTTL := time.Nanosecond
	expectUpdatedCacheTTL := future.Add(oneNanoSecondTTL)

	type args struct {
		eps *discoveryv1.EndpointSlice
	}
	tests := []struct {
		name         string
		tracker      *tracker
		args         args
		wantKey      string
		wantOwnerRef metav1.OwnerReference
		wantState    map[cacheKey]ownerRefTracker
	}{
		{
			name: "Test single entry on empty state. No cache",
			tracker: &tracker{
				ttl: nil,
				state: func() map[cacheKey]ownerRefTracker {
					return emptyState(t)
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: *boilerPlateObjMeta(t),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
					},
				},
			},
			wantKey:      defaultCacheKey,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant},
					endpoints: map[string]*time.Time{
						"host-test":  nil,
						"host-test1": nil,
					},
				}
				state[defaultCacheKey] = nested
				return state
			}(),
		},
		{
			name: "Test single entry on pre-existing state. No cache",
			tracker: &tracker{
				ttl: nil,
				state: func() map[cacheKey]ownerRefTracker {
					state := emptyState(t)
					nested := make(ownerRefTracker)
					nested["test/blaa"] = &hashring{
						tenants: []string{defaultTenant},
						endpoints: map[string]*time.Time{
							"host-test":  nil,
							"host-test1": nil,
						},
					}
					state[defaultCacheKey] = nested
					return state
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: *boilerPlateObjMeta(t),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("exclude1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(false),
							},
						},
					},
				},
			},
			wantKey:      defaultCacheKey,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant},
					endpoints: map[string]*time.Time{
						"host-test1": nil,
					},
				}
				state[defaultCacheKey] = nested
				return state
			}(),
		},
		{
			name: "Test single entry on empty state with TTL",
			tracker: &tracker{
				ttl: &oneHourTTL,
				now: func() time.Time {
					return now
				},
				state: func() map[cacheKey]ownerRefTracker {
					return emptyState(t)
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: *boilerPlateObjMeta(t),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
					},
				},
			},
			wantKey:      defaultCacheKey,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant},
					endpoints: map[string]*time.Time{
						"host-test":  &expectOneHour,
						"host-test1": &expectOneHour,
					},
				}
				state[defaultCacheKey] = nested
				return state
			}(),
		},
		{
			name: "Test single entry on pre-existing state with TTL. Expect no eviction",
			tracker: &tracker{
				ttl: &oneHourTTL,
				now: func() time.Time {
					return now
				},
				state: func() map[cacheKey]ownerRefTracker {
					state := emptyState(t)
					nested := make(ownerRefTracker)
					nested["test/blaa"] = &hashring{
						tenants: []string{defaultTenant},
						endpoints: map[string]*time.Time{
							"host-test":  &expectOneHour,
							"host-test1": &expectOneHour,
						},
					}
					state[defaultCacheKey] = nested
					return state
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: *boilerPlateObjMeta(t),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("exclude1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(false),
							},
						},
					},
				},
			},
			wantKey:      defaultCacheKey,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant},
					endpoints: map[string]*time.Time{
						"host-test":  &expectOneHour,
						"host-test1": &expectOneHour,
					},
				}
				state[defaultCacheKey] = nested
				return state
			}(),
		},
		{
			name: "Test single entry on pre-existing state with TTL. Expect eviction",
			tracker: &tracker{
				ttl: &oneNanoSecondTTL,
				now: func() time.Time {
					return future
				},
				state: func() map[cacheKey]ownerRefTracker {
					state := emptyState(t)
					nested := make(ownerRefTracker)
					nested["test/blaa"] = &hashring{
						tenants: []string{defaultTenant},
						endpoints: map[string]*time.Time{
							"host-test":  &now,
							"host-test1": &now,
						},
					}
					state[defaultCacheKey] = nested
					return state
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: *boilerPlateObjMeta(t),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("exclude1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(false),
							},
						},
					},
				},
			},
			wantKey:      defaultCacheKey,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant},
					endpoints: map[string]*time.Time{
						"host-test1": &expectUpdatedCacheTTL,
					},
				}
				state[defaultCacheKey] = nested
				return state
			}(),
		},
		{
			name: "Test single entry on empty state with label overrides. No cache",
			tracker: &tracker{
				ttl: nil,
				state: func() map[cacheKey]ownerRefTracker {
					return emptyState(t)
				}(),
			},
			args: args{
				eps: &discoveryv1.EndpointSlice{
					ObjectMeta: func() metav1.ObjectMeta {
						m := boilerPlateObjMeta(t)
						m.Labels[TenantIdentifierLabel] = fmt.Sprintf("%s,tenant-2", defaultTenant)
						m.Labels[HashringNameIdentifierLabel] = ""
						return *m
					}(),
					Endpoints: []discoveryv1.Endpoint{
						{
							Hostname: pointer.String("host-test"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
						{
							Hostname: pointer.String("host-test1"),
							Conditions: discoveryv1.EndpointConditions{
								Ready: pointer.Bool(true),
							},
						},
					},
				},
			},
			wantKey:      defaultSVC + "/" + defaultSVC,
			wantOwnerRef: boilerPlateOwnerRef(t),
			wantState: func() map[cacheKey]ownerRefTracker {
				state := emptyState(t)
				nested := make(ownerRefTracker)
				nested["test/blaa"] = &hashring{
					tenants: []string{defaultTenant, "tenant2"},
					endpoints: map[string]*time.Time{
						"host-test":  nil,
						"host-test1": nil,
					},
				}
				state[defaultSVC+"/"+defaultSVC] = nested
				return state
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.tracker.logger = slog.Default()
			err := tt.tracker.saveOrMerge(tt.args.eps)
			if err != nil {
				t.Errorf("tracker.saveOrMerge() error = %v", err)
			}
			assertEqual(t, tt.wantState, tt.tracker.state)
		})
	}
}

func assertEqual(t *testing.T, want map[cacheKey]ownerRefTracker, got map[cacheKey]ownerRefTracker) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("want length of outer map = %v, got %v", len(want), len(got))
	}
	for outerKey, innerKey := range got {
		if _, ok := want[outerKey]; !ok {
			t.Errorf("want key %v not found in want", outerKey)
		}

		if len(innerKey) != len(want[outerKey]) {
			t.Errorf("want length of inner map = %v, got %v", len(want[outerKey]), len(innerKey))
		}

		for uuid, hashring := range innerKey {
			if _, ok := want[outerKey][uuid]; !ok {
				t.Errorf("want uuid %v not found in want", uuid)
			}
			if len(hashring.endpoints) != len(want[outerKey][uuid].endpoints) {
				t.Errorf("want length of endpoints = %v, got %v", len(want[outerKey][uuid].endpoints), len(hashring.endpoints))
			}
			for endpoint, timestamp := range hashring.endpoints {
				if _, ok := want[outerKey][uuid].endpoints[endpoint]; !ok {
					t.Errorf("want endpoint %v not found in want", endpoint)
				}
				if timestamp != nil {
					if timestamp.String() != want[outerKey][uuid].endpoints[endpoint].String() {
						t.Errorf("want timestamp %v not found in want", timestamp)
					}
				}
			}
		}
	}
}

func emptyState(t *testing.T) map[cacheKey]ownerRefTracker {
	t.Helper()
	return make(map[cacheKey]ownerRefTracker)
}

const (
	defaultSVC      = "service1"
	defaultHashring = "default"
	defaultTenant   = "tenant1"

	defaultCacheKey = defaultHashring + "/" + defaultSVC
)

func boilerPlateObjMeta(t *testing.T) *metav1.ObjectMeta {
	t.Helper()
	return &metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			discoveryv1.LabelServiceName: defaultSVC,
			TenantIdentifierLabel:        defaultTenant,
			HashringNameIdentifierLabel:  defaultHashring,
		},
		UID: "blaa",
	}
}

func boilerPlateOwnerRef(t *testing.T) metav1.OwnerReference {
	t.Helper()
	return metav1.OwnerReference{
		APIVersion:         "discovery.k8s.io/v1",
		Kind:               "EndpointSlice",
		Name:               "test",
		UID:                "blaa",
		Controller:         pointer.Bool(true),
		BlockOwnerDeletion: pointer.Bool(true),
	}
}
