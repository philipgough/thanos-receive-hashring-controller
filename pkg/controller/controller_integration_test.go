//go:build integration

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestCreateUpdateDeleteCycleNoCache(t *testing.T) {
	const (
		ns     = "test-no-cache-ns"
		epsOne = "endpoint-slice-one"
		epsTwo = "endpoint-slice-two"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEnv := &envtest.Environment{}

	// start the test environment
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer testEnv.Stop()

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}

	// Create a namespace
	if err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}

	// run the controller in the background
	runController(t, ctx, cfg, ns, nil)

	// Create an EndpointSlice
	eps := buildEndpointSlice(t, ns, epsOne, "hashring-one", "headless-svc", "")
	eps.Endpoints = []discoveryv1.Endpoint{
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-hostname"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-hostname-1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
	}
	// Create the EndpointSlice
	if err := k8sClient.Create(ctx, eps); err != nil {
		t.Fatalf("failed to create EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime := `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.headless-svc.test-no-cache-ns.svc.cluster.local:10901","test-hostname.headless-svc.test-no-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	// Now create another EndpointSlice using the Service label as a hashring name and some hard tenants
	eps2 := buildEndpointSlice(t, ns, epsTwo, "", "hashring-two", "hard-tenant")
	eps2.Endpoints = []discoveryv1.Endpoint{
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-host"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-host-1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("expect-exclude"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(false),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("expect-exclude-also"),
			Conditions: discoveryv1.EndpointConditions{
				Terminating: pointer.Bool(true),
			},
		},
	}
	if err := k8sClient.Create(ctx, eps2); err != nil {
		t.Errorf("failed to create EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.headless-svc.test-no-cache-ns.svc.cluster.local:10901","test-hostname.headless-svc.test-no-cache-ns.svc.cluster.local:10901"]},{"hashring":"hashring-two","tenants":["hard-tenant"],"endpoints":["test-host-1.hashring-two.test-no-cache-ns.svc.cluster.local:10901","test-host.hashring-two.test-no-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	// Now update the EndpointSlice to mark a Pod as terminating and another as not ready
	eps.Endpoints[0].Conditions.Ready = pointer.Bool(false)
	if err := k8sClient.Update(ctx, eps); err != nil {
		t.Fatalf("failed to update EndpointSlice: %v", err)
	}
	eps2.Endpoints[0].Conditions.Terminating = pointer.Bool(true)
	if err := k8sClient.Update(ctx, eps2); err != nil {
		t.Fatalf("failed to update EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.headless-svc.test-no-cache-ns.svc.cluster.local:10901"]},{"hashring":"hashring-two","tenants":["hard-tenant"],"endpoints":["test-host-1.hashring-two.test-no-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	if err := k8sClient.Delete(ctx, eps2); err != nil {
		t.Fatalf("failed to delete EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.headless-svc.test-no-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}
}

func TestCreateUpdateDeleteCycleWithCache(t *testing.T) {
	const (
		ns     = "test-cache-ns"
		epsOne = "endpoint-slice-one"
		epsTwo = "endpoint-slice-two"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEnv := &envtest.Environment{}

	// start the test environment
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer testEnv.Stop()

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}

	// Create a namespace
	if err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}

	fiveMins := time.Minute * 5
	// run the controller in the background
	runController(t, ctx, cfg, ns, &fiveMins)

	// Create an EndpointSlice
	eps := buildEndpointSlice(t, ns, epsOne, "hashring-one", "hashring-one", "")
	eps.Endpoints = []discoveryv1.Endpoint{
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-hostname"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-hostname-1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
	}
	// Create the EndpointSlice
	if err := k8sClient.Create(ctx, eps); err != nil {
		t.Fatalf("failed to create EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime := `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.hashring-one.test-cache-ns.svc.cluster.local:10901","test-hostname.hashring-one.test-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	// Now create another EndpointSlice using the Service label as a hashring name and some hard tenants
	eps2 := buildEndpointSlice(t, ns, epsTwo, "", "hashring-two", "hard-tenant")
	eps2.Endpoints = []discoveryv1.Endpoint{
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-host"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("test-host-1"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(true),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("expect-exclude"),
			Conditions: discoveryv1.EndpointConditions{
				Ready: pointer.Bool(false),
			},
		},
		{
			Addresses: []string{"1.1.1.1"},
			Hostname:  pointer.String("expect-exclude-also"),
			Conditions: discoveryv1.EndpointConditions{
				Terminating: pointer.Bool(true),
			},
		},
	}
	if err := k8sClient.Create(ctx, eps2); err != nil {
		t.Fatalf("failed to create EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.hashring-one.test-cache-ns.svc.cluster.local:10901","test-hostname.hashring-one.test-cache-ns.svc.cluster.local:10901"]},{"hashring":"hashring-two","tenants":["hard-tenant"],"endpoints":["test-host-1.hashring-two.test-cache-ns.svc.cluster.local:10901","test-host.hashring-two.test-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	// Now update the EndpointSlice to mark a Pod as terminating and another as not ready
	eps.Endpoints[0].Conditions.Ready = pointer.Bool(false)
	if err := k8sClient.Update(ctx, eps); err != nil {
		t.Fatalf("failed to update EndpointSlice: %v", err)
	}
	eps2.Endpoints[0].Conditions.Terminating = pointer.Bool(true)
	if err := k8sClient.Update(ctx, eps2); err != nil {
		t.Fatalf("failed to update EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.hashring-one.test-cache-ns.svc.cluster.local:10901","test-hostname.hashring-one.test-cache-ns.svc.cluster.local:10901"]},{"hashring":"hashring-two","tenants":["hard-tenant"],"endpoints":["test-host-1.hashring-two.test-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}

	if err := k8sClient.Delete(ctx, eps2); err != nil {
		t.Fatalf("failed to delete EndpointSlice: %v", err)
	}

	// Assert that the correct data exists in the hashring-config ConfigMap
	expectThisTime = `[{"hashring":"hashring-one","tenants":[],"endpoints":["test-hostname-1.hashring-one.test-cache-ns.svc.cluster.local:10901","test-hostname.hashring-one.test-cache-ns.svc.cluster.local:10901"]}]`
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, ns, expectThisTime, k8sClient); err != nil {
		t.Fatalf("failed to assert EndpointSlice state: %v", err)
	}
}

func pollUntilExpectConfigurationOrTimeout(t *testing.T, ctx context.Context, ns string, expect string, c client.Client) error {

	t.Helper()
	var pollError error

	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Second, false, func(ctx context.Context) (bool, error) {

		cm := &corev1.ConfigMap{}
		err := c.Get(ctx, client.ObjectKey{Name: DefaultConfigMapName, Namespace: ns}, cm)
		if err != nil {
			pollError = fmt.Errorf("failed to query configmap: %s", err)
			return false, nil
		}

		if cm.Data[DefaultConfigMapKey] != expect {
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		return fmt.Errorf("failed to assert contents of hashring-config: %v: %v", err, pollError)
	}

	return nil
}

func runController(t *testing.T, ctx context.Context, cfg *rest.Config, namespace string, cacheTTL *time.Duration) {
	t.Helper()
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err, "Error building kubernetes clientset")
	}

	endpointSliceInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		time.Second*30,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{ServiceLabel: "true"}.String()
		}),
	)

	configMapInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		time.Second*30,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{ConfigMapLabel: "true"}.String()
		}),
	)

	var opts *Options
	if cacheTTL != nil {
		opts = &Options{
			TTL: cacheTTL,
		}
	}

	controller := NewController(
		ctx,
		endpointSliceInformer.Discovery().V1().EndpointSlices(),
		configMapInformer.Core().V1().ConfigMaps(),
		kubeClient,
		namespace,
		opts,
		slog.Default(),
		prometheus.NewRegistry(),
	)

	endpointSliceInformer.Start(ctx.Done())
	configMapInformer.Start(ctx.Done())
	go func() {
		if err := controller.Run(ctx, 1); err != nil {
			t.Fatal(err, "Error running controller")
		}
	}()
}

func buildEndpointSlice(t *testing.T, namespace, name, hashringName, serviceName, tenants string) *discoveryv1.EndpointSlice {
	t.Helper()

	eps := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ServiceLabel:                 "true",
				HashringNameIdentifierLabel:  hashringName,
				discoveryv1.LabelServiceName: serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	if tenants != "" {
		eps.Labels[TenantIdentifierLabel] = tenants
	}
	return eps
}
