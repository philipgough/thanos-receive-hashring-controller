//go:build integration

package sync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/philipgough/hashring-controller/pkg/controller"
	"github.com/prometheus/client_golang/prometheus"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestCreateUpdateCycle(t *testing.T) {
	const (
		ns = "test-write-to-disk-ns"
	)

	tmpdir := t.TempDir()

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
	file := tmpdir + "/" + controller.DefaultConfigMapKey
	runController(t, ctx, cfg, ns, file)

	// Create a ConfigMap
	cm := buildConfigMap(t, ns, controller.DefaultConfigMapName, "hr", `["a", "b", "c"]`, `["t1", "t2"]`)
	if err := k8sClient.Create(ctx, cm); err != nil {
		t.Fatalf("failed to create ConfigMap: %v", err)
	}

	// verify the contents on disk
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, file, cm.Data[controller.DefaultConfigMapKey], k8sClient); err != nil {
		t.Fatalf("failed to assert hashring state: %v", err)
	}

	setTo := `[{"name":"a","tenants":["t1","t2"], "endpoints":["a"]},{"name":"b","tenants":["t3","t4"],"endpoints":["b"]}]`
	cmCopy := cm.DeepCopy()
	cmCopy.Data[controller.DefaultConfigMapKey] = setTo

	if err := k8sClient.Update(ctx, cmCopy); err != nil {
		t.Fatalf("failed to update ConfigMap: %v", err)
	}

	// verify the contents on disk
	if err := pollUntilExpectConfigurationOrTimeout(t, ctx, file, setTo, k8sClient); err != nil {
		t.Fatalf("failed to assert hashring state: %v", err)
	}
}

func runController(t *testing.T, ctx context.Context, cfg *rest.Config, namespace string, filePath string) {
	t.Helper()
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err, "Error building kubernetes clientset")
	}

	configMapInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		time.Second*30,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{controller.ConfigMapLabel: "true"}.String()
		}),
	)

	controller, err := NewController(
		ctx,
		configMapInformer.Core().V1().ConfigMaps(),
		kubeClient,
		namespace,
		slog.Default(),
		prometheus.NewRegistry(),
		Options{
			ConfigMapKey:  controller.DefaultConfigMapKey,
			ConfigMapName: controller.DefaultConfigMapName,
			FilePath:      filePath,
		},
	)
	if err != nil {
		t.Fatal(err, "Error creating controller")
	}

	configMapInformer.Start(ctx.Done())
	go func() {
		if err := controller.Run(ctx, 1); err != nil {
			t.Fatal(err, "Error running controller")
		}
	}()
}

func buildConfigMap(t *testing.T, namespace, name, hashringName, endpoints, tenants string) *corev1.ConfigMap {
	t.Helper()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				controller.ConfigMapLabel: "true",
			},
		},
		Data: map[string]string{
			controller.DefaultConfigMapKey: fmt.Sprintf(`[{"name":"%s","endpoints":%s,"tenants":%s}]`, hashringName, endpoints, tenants),
			//"hashrings.json": fmt.Sprintf(`[{"name":"%s","endpoints":"%s","tenants":%s}]`, hashringName, serviceName, tenants),
		},
	}
	return cm
}

func pollUntilExpectConfigurationOrTimeout(t *testing.T, ctx context.Context, file string, expect string, c client.Client) error {
	t.Helper()
	var pollError error

	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		b, err := os.ReadFile(file)
		if err != nil {
			pollError = fmt.Errorf("failed to read expect file: %s", err)
			return false, nil
		}

		if string(b) != expect {
			pollError = fmt.Errorf("expect file contents do not match expected: expect %s but got %s", expect, string(b))
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		return fmt.Errorf("failed to assert contents of hashring-config: %v: %v", err, pollError)
	}

	return nil
}
