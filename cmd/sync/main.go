package main

import (
	"errors"
	"flag"
	"fmt"
	stdlog "log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/oklog/run"
	"github.com/philipgough/hashring-controller/pkg/controller"
	"github.com/philipgough/hashring-controller/pkg/signals"
	"github.com/philipgough/hashring-controller/pkg/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultListen = ":8080"

	resyncPeriod       = time.Minute
	defaultWaitForSync = false
	defaultPath        = "/var/lib/thanos-receive/hashrings.json"
)

var (
	masterURL  string
	kubeconfig string

	namespace     string
	configMapName string
	configMapKey  string
	pathToWrite   string

	listen string
	wait   bool
)

func main() {
	flag.Parse()
	ctx := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		stdlog.Fatalf("error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		stdlog.Fatalf("error building kubernetes clientset: %s", err.Error())
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	logger = logger.With("component", "hashring-syncer")

	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r, promhttp.HandlerOpts{}))

	configMapInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		resyncPeriod,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fmt.Sprintf("metadata.name=%s", configMapName)
		}),
	)

	controller, err := sync.NewController(
		ctx,
		configMapInformer.Core().V1().ConfigMaps(),
		kubeClient,
		namespace,
		logger,
		r,
		sync.Options{
			ConfigMapKey:  configMapKey,
			ConfigMapName: configMapName,
			FilePath:      pathToWrite,
		},
	)
	if err != nil {
		stdlog.Fatalf("failed to create new controller: %s", err.Error())
	}

	// if wait is set then we want to leverage a postStart hook to ensure contents on disk
	// in order to ensure Thanos starts correctly. We can exit early in any case.
	// todo this might be better handled as a subcommand
	if wait {
		if err := controller.WaitForFileToSync(ctx); err != nil {
			logger.Error("failed to wait for file to sync to disk", "err", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	l, err := net.Listen("tcp", defaultListen)
	if err != nil {
		stdlog.Fatalf("error listening on %s: %s", defaultListen, err.Error())
	}

	var g run.Group
	{
		g.Add(func() error {
			configMapInformer.Start(ctx.Done())
			return controller.Run(ctx, 1)
		},
			func(_ error) {

			},
		)
	}

	{
		g.Add(func() error {
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			if err := http.Serve(l, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("server error: %w", err)
			}
			return nil
		},
			func(error) {
				l.Close()
			},
		)
	}

	if err := g.Run(); err != nil {
		stdlog.Fatalf("error running controller: %s", err.Error())
	}
	logger.Info("controller stopped gracefully")
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&listen, "listen", defaultListen, "The address to listen on")

	flag.StringVar(&namespace, "namespace", metav1.NamespaceDefault, "The namespace to watch")
	flag.StringVar(&configMapName, "name", controller.DefaultConfigMapName, "The ConfigMap to read")
	flag.StringVar(&configMapKey, "key", controller.DefaultConfigMapKey, "The ConfigMap key to read")
	flag.StringVar(&pathToWrite, "path", defaultPath, "The path to write to")

	// the wait flag can be used as a lifecycle hook to ensure criteria is met before starting other applications
	// todo - this might make more sense as a time.Duration or sub-command
	flag.BoolVar(&wait, "wait", defaultWaitForSync, "Should wait for sync. Exits when done or context times out")
}
