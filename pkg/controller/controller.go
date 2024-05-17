package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// EndpointBuilderFN is a function that builds an endpoint string from an Endpoint and an EndpointSlice
type EndpointBuilderFN func(eps *discoveryv1.EndpointSlice, ep discoveryv1.Endpoint) (string, error)

type Trackable interface {
	// EndpointBuilder builds the endpoint string for a given Endpoint.
	EndpointBuilder() EndpointBuilderFN
	// SetValueFor sets the value for the given EndpointSlice and Endpoints in the cache.
	// validEndpoints is a list of Endpoints that are valid (as per the cache behaviour) for the given EndpointSlice.
	SetValueFor(eps *discoveryv1.EndpointSlice, endpoints []string) (interface{}, error)
	// Generate returns the value that will be set in the ConfigMap.
	// A value is a group of EndpointSlices that share the same Service.
	// The caller is responsible for ensuring data deduplication within the output if required.
	Generate(values [][]interface{}) (string, error)
}

const (
	// DefaultServiceLabel is the default label that marks that headless Service.
	// should be watched by the Controller. The value of the label must be "true".
	DefaultServiceLabel = "endpointslice.controller.io/watch"
	// DefaultConfigMapName is the default name for the generated ConfigMap.
	DefaultConfigMapName = "endpointslice-controller-generated-config"
	// DefaultConfigMapLabel is the default label that is used to identify ConfigMaps that are managed by the controller.
	DefaultConfigMapLabel = "endpointslice.controller.io/managed"
	// DefaultConfigMapKey is the default key for data insertion on the generated ConfigMap.
	DefaultConfigMapKey = "data.json"
	// DefaultReSyncPeriod is the default period at which the controller will resync.
	DefaultReSyncPeriod = time.Minute

	// defaultSyncBackOff is the default backoff period for syncService calls.
	defaultSyncBackOff = 1 * time.Second
	// maxSyncBackOff is the max backoff period for sync calls.
	maxSyncBackOff = 1000 * time.Second
)

// Config is the configuration for the Controller.
type Config struct {
	// ReSyncPeriod is the period at which the controller will resync.
	ReSyncPeriod time.Duration
	// ServiceLabel is the label that marks that headless Service should be watched by the Controller.
	// The value of the label must be "true".
	ServiceLabel string
	// ConfigMapName is the name for the generated ConfigMap.
	ConfigMapName string
	// ConfigMapKey is the key for data insertion on the generated ConfigMap.
	ConfigMapKey string
	// ConfigMapLabel is the label that is used to identify ConfigMaps that are managed by the controller.
	ConfigMapLabel string
	// TTL controls the duration for which expired entries are kept in the cache.
	// By default, an endpoint is considered expired if it has become unready due to an involuntary disruption.
	// A nil value disables TTL.
	TTL *time.Duration
}

// DefaultConfig returns the default configuration for the Controller.
func DefaultConfig() Config {
	return Config{
		ReSyncPeriod:   DefaultReSyncPeriod,
		ServiceLabel:   DefaultServiceLabel,
		ConfigMapName:  DefaultConfigMapName,
		ConfigMapKey:   DefaultConfigMapKey,
		ConfigMapLabel: DefaultConfigMapLabel,
		TTL:            nil,
	}
}

// Controller manages selector-based service endpoint slices
type Controller struct {
	// client is the kubernetes client
	client clientset.Interface

	// endpointSliceLister is able to list/get endpoint slices and is populated by the
	// shared informer passed to NewController
	endpointSliceLister discoverylisters.EndpointSliceLister
	// endpointSlicesSynced returns true if the endpoint slice shared informer has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	endpointSlicesSynced cache.InformerSynced

	// configMapLister is able to list/get configmaps and is populated by the
	// shared informer passed to NewController
	configMapLister corelisters.ConfigMapLister
	// configMapSynced returns true if the configmaps shared informer has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	configMapSynced cache.InformerSynced

	// Services that need to be updated. A channel is inappropriate here,
	// because it allows services with lots of pods to be serviced much
	// more often than services with few pods; it also would cause a
	// service that's inserted multiple times to be processed more than
	// necessary.
	queue workqueue.RateLimitingInterface

	// workerLoopPeriod is the time between worker runs. The workers
	// process the queue of service and pod changes
	workerLoopPeriod time.Duration

	//tracker is used to track the status of the controller
	tracker *tracker
	// trackable is the implementation of the Trackable interface that will be cached and generated
	trackable Trackable
	namespace string
	config    Config
	logger    *slog.Logger
	metrics   *metrics
}

func NewController(
	endpointSliceInformer discoveryinformers.EndpointSliceInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	client clientset.Interface,
	trackable Trackable,
	namespace string,
	config Config,
	logger *slog.Logger,
	registry *prometheus.Registry,
) *Controller {

	ctrlMetrics := newMetrics()
	if registry == nil {
		registry.MustRegister(ctrlMetrics.configMapHash, ctrlMetrics.configMapLastChangeSuccessTime)
	}

	c := &Controller{
		client: client,
		// This is similar to the DefaultControllerRateLimiter, just with a
		// significantly higher default backoff (1s vs 5ms). A more significant
		// rate limit back off here helps ensure that the Controller does not
		// overwhelm the API Server.
		queue: workqueue.NewRateLimitingQueueWithConfig(
			workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(defaultSyncBackOff, maxSyncBackOff),
				// 10 qps, 100 bucket size.
				// This is only for retry speed and is only the overall factor (not per item).
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
			workqueue.RateLimitingQueueConfig{Name: "endpoint_slice"}),
		workerLoopPeriod: time.Second,
		namespace:        namespace,
		config:           config,
		tracker:          newTracker(config.TTL, logger.With("component", "endpointslice_tracker"), trackable),
		logger:           logger,
		metrics:          ctrlMetrics,
	}

	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.handleObject,
		UpdateFunc: func(old, new interface{}) {
			newCM := new.(*corev1.ConfigMap)
			oldCM := old.(*corev1.ConfigMap)
			if newCM.ResourceVersion == oldCM.ResourceVersion {
				// Periodic resync will send update events for all known ConfigMaps.
				// Two different versions of the same ConfigMaps will always have different RVs.
				return
			}
			c.handleObject(new)
		},
		DeleteFunc: c.handleObject,
	})

	c.configMapLister = configMapInformer.Lister()
	c.configMapSynced = configMapInformer.Informer().HasSynced

	endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onEndpointSliceAdd,
		UpdateFunc: c.onEndpointSliceUpdate,
		DeleteFunc: c.onEndpointSliceDelete,
	})

	c.endpointSliceLister = endpointSliceInformer.Lister()
	c.endpointSlicesSynced = endpointSliceInformer.Informer().HasSynced

	return c
}

// EnsureConfigMapExists ensures that the controller's configmap exists or tries to create it if not.
// If the configmap already exists, it will be returned. If it does not exist, it will be created.
// The returned boolean indicates whether the configmap was created or not.
func (c *Controller) EnsureConfigMapExists(ctx context.Context, data string) (*corev1.ConfigMap, bool, error) {
	var cm *corev1.ConfigMap
	var pollError error
	preExists := true
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		var fetchError error
		cm, fetchError = c.client.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.config.ConfigMapName, metav1.GetOptions{})
		if fetchError != nil {

			if errors.IsNotFound(fetchError) {
				preExists = false
				cm, pollError = c.client.CoreV1().ConfigMaps(c.namespace).
					Create(ctx, c.newConfigMap(data, nil), metav1.CreateOptions{})
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return cm, preExists, fmt.Errorf("failed to ensure configmap exists: %w: %w", err, pollError)
	}

	return cm, preExists, nil
}

// InformersFromConfig is a helper that returns the informers for the controller based on the given configuration.
func InformersFromConfig(
	client kubernetes.Interface,
	namespace string,
	config Config,
) (kubeinformers.SharedInformerFactory, kubeinformers.SharedInformerFactory) {

	endpointSliceInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		client,
		config.ReSyncPeriod,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{config.ServiceLabel: "true"}.String()
		}),
	)

	configMapInformer := kubeinformers.NewSharedInformerFactoryWithOptions(
		client,
		config.ReSyncPeriod,
		kubeinformers.WithNamespace(namespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.Set{config.ConfigMapLabel: "true"}.String()
		}),
	)
	return endpointSliceInformer, configMapInformer
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the queue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	c.logger.Info("starting hashring controller")
	c.logger.Info("waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.endpointSlicesSynced, c.configMapSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	c.logger.Info("starting workers", slog.Int("count", workers))
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	c.logger.Info("started workers")
	<-ctx.Done()
	c.logger.Info("shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the queue.
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the queue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.queue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the queue knows we have finished processing this item.
		// We also must remember to call Forget if we do not want this work item being re-queued.
		// For example, we do not call Forget if a transient error occurs, instead the item is
		// put back on the queue and attempted again after a back-off period.
		defer c.queue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the queue which are of the form namespace/name.
		// We do this as the delayed nature of the queue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the queue.
		if key, ok = obj.(string); !ok {
			// As the item in the queue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.queue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the EndpointSlice resource to be synced.
		if err := c.syncHandler(ctx, key); err != nil {
			// Put the item back on the queue to handle any transient errors.
			c.queue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.queue.Forget(obj)
		c.logger.Debug("successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// enqueueEndpointSlice takes a EndpointSlice resource
// It converts it into a namespace/name string which is then put onto the queue.
// This method should *not* be passed resources of any type other than EndpointSlice.
func (c *Controller) enqueueEndpointSlice(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

// syncHandler compares the actual state with the desired, and attempts to converge the two.
func (c *Controller) syncHandler(ctx context.Context, key string) error {
	c.logger.Debug("syncHandler called", "resourceName", key)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the EndpointSlice resource with this namespace/name
	eps, err := c.endpointSliceLister.EndpointSlices(namespace).Get(name)
	if err != nil {
		// The EndpointSlice resource may no longer exist, in which case we stop processing.
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("EndpointSlice '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	epsCopied := eps.DeepCopy()
	if err := c.tracker.saveOrMerge(epsCopied); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncHandler failed to reconcile resource key: %s", key))
		return err
	}

	return c.reconcile(ctx)
}

// reconcile compares the actual state with the desired, and attempts to converge the two.
func (c *Controller) reconcile(ctx context.Context) error {
	output, owners, err := c.tracker.generate()
	if err != nil {
		// we log and return here because we don't want to requeue the item
		c.logger.Error("failed to generate output during reconcile", "error", err.Error())
		return nil
	}

	// Get the ConfigMap or create it if it doesn't exist.
	cm, err := c.configMapLister.ConfigMaps(c.namespace).Get(c.config.ConfigMapName)
	if errors.IsNotFound(err) {
		cm, err = c.client.CoreV1().ConfigMaps(c.namespace).Create(ctx,
			c.newConfigMap(output, owners), metav1.CreateOptions{})
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	if cm.Data[c.config.ConfigMapKey] != output {
		_, err = c.client.CoreV1().ConfigMaps(c.namespace).Update(
			ctx, c.newConfigMap(output, owners), metav1.UpdateOptions{})
	}

	// If an error occurs during Update, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	c.metrics.configMapLastChangeSuccessTime.Set(float64(time.Now().Unix()))
	// todo add as hook
	//c.metrics.configMapLastChangeSuccessTime.Set(hashAsMetricValue(hashringConfig))
	return nil
}

// newConfigMap creates a new ConfigMap
// It sets a label so that the controller can watch for changes to the ConfigMap
func (c *Controller) newConfigMap(data string, owners []metav1.OwnerReference) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.config.ConfigMapName,
			Namespace: c.namespace,
			Labels: map[string]string{
				c.config.ConfigMapLabel: "true",
			},
			OwnerReferences: owners,
		},
		Data: map[string]string{
			c.config.ConfigMapKey: data,
		},
		BinaryData: nil,
	}
}

func (c *Controller) onEndpointSliceAdd(obj interface{}) {
	eps, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		c.logger.Error("unexpected object type", "type", fmt.Sprintf("%T", obj))
		return
	}

	if !c.shouldEnqueue(eps) {
		return
	}
	c.enqueueEndpointSlice(eps)
}

func (c *Controller) onEndpointSliceUpdate(oldObj, newObj interface{}) {
	newEps := newObj.(*discoveryv1.EndpointSlice)
	oldEps := oldObj.(*discoveryv1.EndpointSlice)

	if !c.shouldEnqueue(newEps) {
		return
	}

	if newEps.ResourceVersion == oldEps.ResourceVersion {
		// Periodic resync will send update events for all known EndpointSlice.
		// Two different versions will always have different RVs.
		return
	}

	c.enqueueEndpointSlice(newEps)
}

func (c *Controller) onEndpointSliceDelete(obj interface{}) {
	eps, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		c.logger.Error("unexpected object type", "type", fmt.Sprintf("%T", obj))
		return
	}

	if !c.shouldEnqueue(eps) {
		// filter out EndpointSlice not owned by a Service
		return
	}
	if ok, err := c.tracker.evict(eps); !ok || err != nil {
		return
	}
	c.reconcile(context.Background())
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the EndpointSlice resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that EndpointSlice resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool

	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		c.logger.Info("recovered deleted object", "resourceName", object.GetName())
	}
	c.logger.Info("processing object", "object", object.GetName())
	ownerRefs := object.GetOwnerReferences()

	for _, ownerRef := range ownerRefs {
		if ownerRef.Kind != "EndpointSlice" {
			return
		}

		eps, err := c.endpointSliceLister.EndpointSlices(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			c.logger.Info("ignore orphaned object",
				"object", object.GetName(), "owner", ownerRef.Name)
			return
		}
		c.enqueueEndpointSlice(eps)
	}
}

func (c *Controller) shouldEnqueue(eps *discoveryv1.EndpointSlice) bool {
	// we need at least the service name label to be present
	if value, ok := eps.GetLabels()[discoveryv1.LabelServiceName]; !ok || value == "" {
		// add debug log
		return false
	}
	return true
}
