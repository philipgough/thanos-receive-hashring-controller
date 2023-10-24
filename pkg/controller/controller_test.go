package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/philipgough/hashring-controller/pkg/config"
	"github.com/prometheus/client_golang/prometheus"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/pointer"
)

const (
	testServiceName = "test-svc"
)

var (
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }
)

type fixture struct {
	t *testing.T

	kubeclient *fake.Clientset
	// objects to put in the store
	endpointSliceLister []*discoveryv1.EndpointSlice
	configMapLister     []*corev1.ConfigMap
	// actions expected to happen on the client.
	actions []core.Action
	// objects are preloaded into NewSimpleFake.
	objects []runtime.Object
	cache   *tracker
}

func endpointsFixture() []discoveryv1.Endpoint {
	return []discoveryv1.Endpoint{
		{
			Hostname: pointer.String("test1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Hostname: pointer.String("test2"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Hostname: pointer.String("always-exclude-terminating"),
			Conditions: discoveryv1.EndpointConditions{
				Terminating: pointer.Bool(true),
				Ready:       pointer.Bool(true),
			},
		},
	}
}

func hashringsAsStringData(t *testing.T, hashrings config.Hashrings) string {
	t.Helper()
	b, err := json.Marshal(hashrings)
	if err != nil {
		t.Errorf("failed to marshal hashrings: %v", err)
	}
	return string(b)
}

func TestDoNothing(t *testing.T) {
	f := newFixture(t)
	eps := newEndpointSlice("test", endpointsFixture())
	f.objects = append(f.objects, eps)
	f.run(context.Background(), getKey(eps, t))
}

func TestCreatesConfigMap(t *testing.T) {
	f := newFixture(t)
	eps := newEndpointSlice("test", endpointsFixture())

	f.endpointSliceLister = append(f.endpointSliceLister, eps)
	f.objects = append(f.objects, eps)

	ctrl := &Controller{configMapName: DefaultConfigMapName, namespace: metav1.NamespaceDefault}
	expectData := hashringsAsStringData(t, config.Hashrings{
		{
			HashringSpec: config.HashringSpec{
				Name:    testServiceName,
				Tenants: []string{},
			},
			Endpoints: []string{
				"test1." + testServiceName + ".default.svc.cluster.local:10901",
				"test2." + testServiceName + ".default.svc.cluster.local:10901",
			},
		},
	})
	expectConfigMap := ctrl.newConfigMap(expectData, []metav1.OwnerReference{buildOwnerReference(eps)})
	f.expectCreateConfigMapAction(expectConfigMap)
	f.run(context.Background(), getKey(eps, t))
}

func TestUpdatesConfigMap(t *testing.T) {
	f := newFixture(t)
	eps := newEndpointSlice("test", endpointsFixture())
	_, ctx := ktesting.NewTestContext(t)

	ctrl := &Controller{
		configMapName: DefaultConfigMapName,
		namespace:     metav1.NamespaceDefault,
		logger:        slog.Default(),
	}
	data := hashringsAsStringData(t, config.Hashrings{
		{
			HashringSpec: config.HashringSpec{
				Name:    testServiceName,
				Tenants: []string{},
			},
			Endpoints: []string{
				"test1." + testServiceName + ".default.svc.cluster.local:10901",
				"test2." + testServiceName + ".default.svc.cluster.local:10901",
			},
		},
	})
	cm := ctrl.newConfigMap(data, []metav1.OwnerReference{buildOwnerReference(eps)})

	updatedEPS := append(eps.Endpoints, discoveryv1.Endpoint{
		Hostname: pointer.String("hello"),
		Conditions: discoveryv1.EndpointConditions{
			Ready: pointer.Bool(true),
		},
	})
	eps.Endpoints = updatedEPS

	updatedData := hashringsAsStringData(t, config.Hashrings{
		{
			HashringSpec: config.HashringSpec{
				Name:    testServiceName,
				Tenants: []string{},
			},
			Endpoints: []string{
				"hello." + testServiceName + ".default.svc.cluster.local:10901",
				"test1." + testServiceName + ".default.svc.cluster.local:10901",
				"test2." + testServiceName + ".default.svc.cluster.local:10901",
			},
		},
	})
	expectCM := ctrl.newConfigMap(updatedData, []metav1.OwnerReference{buildOwnerReference(eps)})

	f.endpointSliceLister = append(f.endpointSliceLister, eps)
	f.objects = append(f.objects, eps)
	f.configMapLister = append(f.configMapLister, cm)
	f.objects = append(f.objects, cm)

	f.expectUpdateConfigMapAction(expectCM)
	f.run(ctx, getKey(eps, t))
}

func TestUpdateConfigMapWithNotReady(t *testing.T) {
	f := newFixture(t)
	eps := newEndpointSlice("test", endpointsFixture())

	ctrl := &Controller{
		configMapName: DefaultConfigMapName,
		namespace:     metav1.NamespaceDefault,
		logger:        slog.Default(),
	}
	data := hashringsAsStringData(t, config.Hashrings{
		{
			HashringSpec: config.HashringSpec{
				Name:    testServiceName,
				Tenants: []string{},
			},
			Endpoints: []string{
				"test1." + testServiceName + ".default.svc.cluster.local:10901",
				"test2." + testServiceName + ".default.svc.cluster.local:10901",
			},
		},
	})
	cm := ctrl.newConfigMap(data, []metav1.OwnerReference{buildOwnerReference(eps)})

	eps.Endpoints[0] = discoveryv1.Endpoint{
		Hostname: pointer.String("test1"),
		Conditions: discoveryv1.EndpointConditions{
			Ready: pointer.Bool(false),
		},
	}

	updatedEPS := append(eps.Endpoints, discoveryv1.Endpoint{
		Hostname: pointer.String("hello"),
		Conditions: discoveryv1.EndpointConditions{
			Ready: pointer.Bool(true),
		},
	})

	eps.Endpoints = updatedEPS

	updatedData := hashringsAsStringData(t, config.Hashrings{
		{
			HashringSpec: config.HashringSpec{
				Name:    testServiceName,
				Tenants: []string{},
			},
			Endpoints: []string{
				"hello." + testServiceName + ".default.svc.cluster.local:10901",
				"test2." + testServiceName + ".default.svc.cluster.local:10901",
			},
		},
	})
	expectCM := ctrl.newConfigMap(updatedData, []metav1.OwnerReference{buildOwnerReference(eps)})

	f.endpointSliceLister = append(f.endpointSliceLister, eps)
	f.objects = append(f.objects, eps)
	f.configMapLister = append(f.configMapLister, cm)
	f.objects = append(f.objects, cm)

	f.expectUpdateConfigMapAction(expectCM)
	f.run(context.Background(), getKey(eps, t))
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{}
	f.t = t
	f.objects = []runtime.Object{}
	return f
}

func (f *fixture) newController(ctx context.Context) (*Controller, informers.SharedInformerFactory) {
	f.kubeclient = fake.NewSimpleClientset(f.objects...)

	k8sI := informers.NewSharedInformerFactory(f.kubeclient, noResyncPeriodFunc())

	c := NewController(
		ctx,
		k8sI.Discovery().V1().EndpointSlices(),
		k8sI.Core().V1().ConfigMaps(),
		f.kubeclient,
		metav1.NamespaceDefault,
		nil,
		slog.Default(),
		prometheus.NewRegistry(),
	)

	if f.cache != nil {
		c.tracker = f.cache
	}

	c.endpointSlicesSynced = alwaysReady
	c.configMapSynced = alwaysReady

	for _, eps := range f.endpointSliceLister {
		k8sI.Discovery().V1().EndpointSlices().Informer().GetIndexer().Add(eps)
	}

	for _, d := range f.configMapLister {
		k8sI.Core().V1().ConfigMaps().Informer().GetIndexer().Add(d)
	}

	return c, k8sI
}

func (f *fixture) run(ctx context.Context, key string) {
	f.runController(ctx, key, true, false)
}

func (f *fixture) runExpectError(ctx context.Context, key string) {
	f.runController(ctx, key, true, true)
}

func (f *fixture) runController(ctx context.Context, key string, startInformers bool, expectError bool) {
	c, k8sI := f.newController(ctx)
	if startInformers {
		k8sI.Start(ctx.Done())
	}

	err := c.syncHandler(ctx, key)
	if !expectError && err != nil {
		f.t.Errorf("error syncing endpointslice: %v", err)
	} else if expectError && err == nil {
		f.t.Error("expected error syncing endpointslice, got nil")
	}

	actions := filterInformerActions(f.kubeclient.Actions())
	for i, action := range actions {
		if len(f.actions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.actions), actions[i:])
			break
		}

		expectedAction := f.actions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.actions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.actions)-len(actions), f.actions[len(actions):])
	}
}

// checkAction verifies that expected and actual actions are equal and both have
// same attached resources
func checkAction(expected, actual core.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) && actual.GetSubresource() == expected.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	switch a := actual.(type) {
	case core.CreateActionImpl:
		e, _ := expected.(core.CreateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case core.UpdateActionImpl:
		e, _ := expected.(core.UpdateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case core.PatchActionImpl:
		e, _ := expected.(core.PatchActionImpl)
		expPatch := e.GetPatch()
		patch := a.GetPatch()

		if !reflect.DeepEqual(expPatch, patch) {
			t.Errorf("Action %s %s has wrong patch\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expPatch, patch))
		}
	default:
		t.Errorf("Uncaptured Action %s %s, you should explicitly add a case to capture it",
			actual.GetVerb(), actual.GetResource().Resource)
	}
}

// filterInformerActions filters list and watch actions for testing resources.
// Since list and watch don't change resource state we can filter it to lower
// noise level in our tests.
func filterInformerActions(actions []core.Action) []core.Action {
	var ret []core.Action
	for _, action := range actions {
		if len(action.GetNamespace()) == 0 &&
			(action.Matches("list", "endpointslices") ||
				action.Matches("watch", "endpointslices") ||
				action.Matches("list", "configmaps") ||
				action.Matches("watch", "configmaps")) {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

func (f *fixture) expectCreateConfigMapAction(cm *corev1.ConfigMap) {
	f.actions = append(f.actions, core.NewCreateAction(schema.GroupVersionResource{Resource: "configmaps"}, cm.Namespace, cm))
}

func (f *fixture) expectCreateEndpointSliceAction(eps *discoveryv1.EndpointSlice) {
	f.actions = append(f.actions, core.NewCreateAction(schema.GroupVersionResource{Resource: "endpointslices"}, eps.Namespace, eps))
}

func (f *fixture) expectUpdateConfigMapAction(cm *corev1.ConfigMap) {
	f.actions = append(f.actions, core.NewUpdateAction(schema.GroupVersionResource{Resource: "configmaps"}, cm.Namespace, cm))
}

func (f *fixture) expectUpdateEndpointSliceAction(eps *discoveryv1.EndpointSlice) {
	f.actions = append(f.actions, core.NewUpdateAction(schema.GroupVersionResource{Resource: "endpointslices"}, eps.Namespace, eps))
}

func getKey(eps *discoveryv1.EndpointSlice, t *testing.T) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(eps)
	if err != nil {
		t.Errorf("Unexpected error getting key for endpointslice %v: %v", eps.Name, err)
		return ""
	}
	return key
}

func newEndpointSlice(name string, endpoints []discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
			Labels: map[string]string{
				discoveryv1.LabelServiceName: testServiceName,
			},
		},
		Endpoints: endpoints,
	}
}

func TestControllerGenerate(t *testing.T) {
	tests := []struct {
		name     string
		tracker  *tracker
		expected config.Hashrings
	}{
		{
			name: "Test single hashring with multiple endpoints and tenants",
			tracker: &tracker{
				mut: sync.RWMutex{},
				state: map[cacheKey]ownerRefTracker{
					defaultSVC + "/" + defaultSVC: {
						"some-uid": &hashring{
							tenants: []string{"tenant1", "tenant2"},
							endpoints: map[string]*time.Time{
								"endpoint1": nil,
								"endpoint2": nil,
							},
						},
					},
				},
			},
			expected: config.Hashrings{
				{
					HashringSpec: config.HashringSpec{
						Name:    defaultSVC,
						Tenants: []string{"tenant1", "tenant2"},
					},
					Endpoints: []string{"endpoint1", "endpoint2"},
				},
			},
		},
		{
			name: "Test multiple hashring with multiple endpoints and tenants",
			tracker: &tracker{
				mut: sync.RWMutex{},
				state: map[cacheKey]ownerRefTracker{
					defaultSVC + "/" + defaultSVC: {
						"some-uid": &hashring{
							tenants: []string{"tenant1", "tenant2"},
							endpoints: map[string]*time.Time{
								"endpoint1": nil,
								"endpoint2": nil,
							},
						},
					},
					defaultHashring: {
						"some-other-uid": &hashring{
							tenants: []string{"an-other"},
							endpoints: map[string]*time.Time{
								"endpoint10": nil,
								"endpoint20": nil,
							},
						},
					},
				},
			},
			expected: config.Hashrings{
				{
					HashringSpec: config.HashringSpec{
						Name:    defaultHashring,
						Tenants: []string{"an-other"},
					},
					Endpoints: []string{"endpoint10", "endpoint20"},
				},
				{
					HashringSpec: config.HashringSpec{
						Name:    defaultSVC,
						Tenants: []string{"tenant1", "tenant2"},
					},
					Endpoints: []string{"endpoint1", "endpoint2"},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := &Controller{
				tracker: test.tracker,
				buildFQDN: func(hostname, svc string) string {
					return hostname
				},
				logger: slog.Default(),
			}
			hashrings, _ := ctrl.generate(test.tracker.state)
			if !reflect.DeepEqual(hashrings, test.expected) {
				t.Errorf("Unexpected result: %v (expected: %v)", hashrings, test.expected)
			}
		})
	}
}

func buildOwnerReference(eps *discoveryv1.EndpointSlice) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: discoveryv1.SchemeGroupVersion.String(),
		Kind:       "EndpointSlice",
		Name:       eps.GetName(),
		UID:        eps.GetUID(),
	}
}
