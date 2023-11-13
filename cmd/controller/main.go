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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gopkg.in/yaml.v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultListen   = ":8080"
	defaultLogLevel = "info"

	resyncPeriod = time.Minute

	defaultInitNodeCount = 6
)

var (
	masterURL  string
	kubeconfig string
	namespace  string

	listen     string
	logLevel   string
	configFile string
)

type config struct {
	// SyncState consumes a pre-existing ConfigMap if it exists on startup.
	// Defaults to true.
	SyncState *bool `yaml:"sync_state"`
	// WriteInitialState writes a dummy ConfigMap to the filesystem on startup.
	// Defaults to true. This field is ignored if SyncState is true.
	WriteInitialState *bool `yaml:"write_initial_state"`
	// WriteInitialNodeCount is the number of nodes to write to the dummy ConfigMap.
	// It defaults to 6 to support a replication factor of 5 out of the box
	WriteInitialNodeCount int `yaml:"write_initial_node_count"`
}

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

	l, err := net.Listen("tcp", defaultListen)
	if err != nil {
		stdlog.Fatalf("error listening on %s: %s", defaultListen, err.Error())
	}

	ctrlCfg := defaultConfig()
	if configFile != "" {
		data, err := os.ReadFile(configFile)
		if err != nil {
			stdlog.Fatalf("error reading config file: %s", err.Error())
		}
		var cfg config
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			stdlog.Fatalf("error parsing config file: %s", err.Error())
		}
		ctrlCfg = buildConfigFromFile(cfg)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: getLogLevel(logLevel),
	}))
	logger = logger.With("component", "hashring-controller")

	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r, promhttp.HandlerOpts{}))

	endpointSliceInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		resyncPeriod,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{controller.ServiceLabel: "true"}.String()
		}),
	)

	configMapInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		resyncPeriod,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{controller.ConfigMapLabel: "true"}.String()
		}),
	)

	controller := controller.NewController(
		ctx,
		endpointSliceInformer.Discovery().V1().EndpointSlices(),
		configMapInformer.Core().V1().ConfigMaps(),
		kubeClient,
		namespace,
		nil,
		logger,
		r,
	)

	if toBool(ctrlCfg.WriteInitialState) {
		if err := controller.EnsureConfigMapExists(ctx, "receiver", 6); err != nil {
			stdlog.Fatalf("error ensuring configmap exists: %s", err.Error())
		}
	}

	var g run.Group
	{
		g.Add(func() error {
			endpointSliceInformer.Start(ctx.Done())
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
	flag.StringVar(&namespace, "namespace", metav1.NamespaceDefault, "The namespace to watch")
	flag.StringVar(&listen, "listen", defaultListen, "The address to listen on")
	flag.StringVar(&logLevel, "log-level", defaultLogLevel, "The log level to use")
	flag.StringVar(&configFile, "config", "", "Path to a config file")

}

func defaultConfig() config {
	return config{
		SyncState:             boolPtr(true),
		WriteInitialState:     boolPtr(true),
		WriteInitialNodeCount: defaultInitNodeCount,
	}
}

func buildConfigFromFile(cfg config) config {
	conf := defaultConfig()
	if cfg.SyncState != nil {
		conf.SyncState = cfg.SyncState
	}
	if cfg.WriteInitialState != nil {
		conf.WriteInitialState = cfg.WriteInitialState
	}
	if cfg.WriteInitialNodeCount != 0 {
		conf.WriteInitialNodeCount = cfg.WriteInitialNodeCount
	}
	return conf
}

func getLogLevel(lvl string) slog.Level {
	switch lvl {
	case "info":
		return slog.LevelInfo
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		stdlog.Printf("unknown log level %q, defaulting to info", lvl)
		return slog.LevelInfo
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func toBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
