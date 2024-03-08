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

	"github.com/philipgough/hashring-controller/pkg/controller"
	"github.com/philipgough/hashring-controller/pkg/signals"
	"github.com/philipgough/hashring-controller/pkg/thanos"

	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultListen     = ":8080"
	defaultLogLevel   = "info"
	defaultTTLSeconds = 600
	// defaultInitialHashring is the default initial hashring to use if the ConfigMap does not exist.
	// This allows Thanos to pass initial validation and start the process.
	defaultInitialHashring = `[{"endpoints":["127.0.0.1:1234","127.0.0.1:1234","127.0.0.1:12345"]}]`
)

var (
	masterURL  string
	kubeconfig string
	namespace  string

	listen     string
	logLevel   string
	ttlSeconds int

	hashring string
	static   bool
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

	l, err := net.Listen("tcp", defaultListen)
	if err != nil {
		stdlog.Fatalf("error listening on %s: %s", defaultListen, err.Error())
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

	config := controller.DefaultConfig()
	endpointSliceInformer, configMapInformer := controller.InformersFromConfig(kubeClient, namespace, config)

	controller := controller.NewController(
		endpointSliceInformer.Discovery().V1().EndpointSlices(),
		configMapInformer.Core().V1().ConfigMaps(),
		kubeClient,
		thanos.NewReceiveHashringController(nil, nil),
		namespace,
		controller.DefaultConfig(),
		logger,
		r,
	)

	// Check for existence of ConfigMap or create it if it doesn't exist.
	cm, alreadyExists, err := controller.EnsureConfigMapExists(ctx, hashring)
	if err != nil {
		stdlog.Fatalf("error ensuring configmap exists: %s", err.Error())
	}

	if !alreadyExists {
		// assume we are in bootstrap mode, and the ConfigMap did not exist.
		// Give some time for Thanos Receive to start up and validate the ConfigMap.
		logger.Info("ConfigMap did not exist, waiting for Thanos Receive to start up")
		<-time.After(time.Minute)
	}

	if static {
		logger.Warn("static mode enabled, no reconciliation will occur")
		cm.Data[config.ConfigMapKey] = hashring
		if _, err := kubeClient.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			stdlog.Fatalf("error updating configmap: %s", err.Error())
		}
		for {
			select {
			case <-ctx.Done():
				logger.Info("controller exited gracefully from static mode")
				os.Exit(0)
			}
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
	os.Exit(0)
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "",
		"Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "",
		"The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&namespace, "namespace", metav1.NamespaceDefault,
		"The namespace to watch")
	flag.StringVar(&listen, "listen", defaultListen,
		"The address to listen on.")
	flag.StringVar(&logLevel, "log-level", defaultLogLevel,
		"The log level to use.")
	flag.IntVar(&ttlSeconds, "ttl", defaultTTLSeconds,
		"The number of seconds to cache endpoints which have become unready due to involuntary disruption.")
	flag.StringVar(
		&hashring,
		"hashring",
		defaultInitialHashring,
		"Specifies the initial hashring to use if the ConfigMap does not exist.")
	flag.BoolVar(
		&static,
		"static",
		false,
		"When enabled, the ConfigMap will be populated with the value of 'hashring' flag. No reconciliation occurs.")
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
