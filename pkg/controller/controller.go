package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/philipgough/hashring-controller/pkg/config"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
)

const (
	// ServiceLabel *must* be set to 'true' on a corev1.Service that should be watched by the Controller
	ServiceLabel = "hashring.controller.io/watch"
	// HashringNameIdentifierLabel is an optional label that is used by the controller to identify the hashring name.
	// A missing/empty value defaults to the name of the Service.
	HashringNameIdentifierLabel = "hashring.controller.io/hashring"
	// TenantIdentifierLabel is an optional label that is used by the controller to identify a tenant for the hashring
	// When relying on default behaviour for the controller, the absence of this label
	// on a Service will result in an empty tenant list which matches all tenants providing soft-tenancy
	TenantIdentifierLabel = "hashring.controller.io/tenant"
	// AlgorithmIdentifierLabel is the label that is used by the controller to identify the hashring algorithm
	// When relying on default behaviour for the controller, the absence of this label
	// on a Service will result in the use of config.DefaultAlgorithm
	AlgorithmIdentifierLabel = "hashring.controller.io/hashing-algorithm"

	// DefaultConfigMapName is the default name for the generated ConfigMap
	DefaultConfigMapName = "hashring-controller-generated-config"
	// ConfigMapLabel is the label that is used to identify configmaps that is managed by the controller
	ConfigMapLabel = "hashring.controller.io/managed"

	// DefaultConfigMapKey is the default key for the generated ConfigMap
	DefaultConfigMapKey  = "hashrings.json"
	defaultPort          = "10901"
	defaultClusterDomain = "cluster.local"

	// defaultSyncBackOff is the default backoff period for syncService calls.
	defaultSyncBackOff = 1 * time.Second
	// maxSyncBackOff is the max backoff period for sync calls.
	maxSyncBackOff = 1000 * time.Second
)

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

	// configMapName is the name of the configmap that the controller will generate
	configMapName string
	// configMapKey is the key of the configmap that the controller will generate
	configMapKey string
	// port is the port that the controller will generate a hashring for
	port string
	// clusterDomain is the cluster domain that the controller will generate a hashring for
	clusterDomain string
	// namespace is the namespace of the configmap that the controller will generate
	namespace string
	// buildFQDN is a function that builds a FQDN from a hostname and a service name
	buildFQDN func(hostname, serviceName string) string
	logger    *slog.Logger
	metrics   *metrics
}

// Options provides a source to override default controller behaviour
type Options struct {
	// TTL controls the duration for which expired entries are kept in the cache
	// If not set, no TTL will be applied
	// Only Endpoints that have become unready due to an involuntary disruption are cached
	// Terminated Endpoints are removed from the hashring in all cases
	TTL *time.Duration
	// ConfigMapKey is the key for hashring config on the generated ConfigMap
	ConfigMapKey *string
	// ConfigMapName is the name of the generated ConfigMap
	ConfigMapName *string
	// Port is the port that the hashring will be generated for
	Port *string
	// ClusterDomain is the cluster domain that the hashring will be generated for
	ClusterDomain *string
}

func NewController(
	ctx context.Context,
	endpointSliceInformer discoveryinformers.EndpointSliceInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	client clientset.Interface,
	namespace string,
	opts *Options,
	logger *slog.Logger,
	registry *prometheus.Registry,
) *Controller {
	opts = buildOpts(opts)

	ctrlMetrics := newMetrics()
	if registry == nil {
		registry.MustRegister(ctrlMetrics.configMapHash, ctrlMetrics.configMapLastChangeSuccessTime)
	}

	c := &Controller{
		client:        client,
		configMapName: *opts.ConfigMapName,
		configMapKey:  *opts.ConfigMapKey,
		port:          *opts.Port,
		clusterDomain: *opts.ClusterDomain,
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
		tracker:          newTracker(opts.TTL, logger.With("component", "endpointslice_tracker")),
		logger:           logger,
		metrics:          ctrlMetrics,
	}

	c.buildFQDN = c.defaultBuildFQDN

	endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onEndpointSliceAdd,
		UpdateFunc: c.onEndpointSliceUpdate,
		DeleteFunc: c.onEndpointSliceDelete,
	})

	c.endpointSliceLister = endpointSliceInformer.Lister()
	c.endpointSlicesSynced = endpointSliceInformer.Informer().HasSynced

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

	return c
}

// EnsureConfigMapExists ensures that the controller's configmap exists or tries to create it with an empty hashring
func (c *Controller) EnsureConfigMapExists(ctx context.Context) error {
	var pollError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, false, func(ctx context.Context) (bool, error) {
		_, err := c.client.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.configMapName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				_, pollError = c.client.CoreV1().ConfigMaps(c.namespace).Create(
					ctx, c.newConfigMap(`[{"hashring":"default","endpoints":[],"tenants":[]}]`, nil), metav1.CreateOptions{})
				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed to ensure configmap exists: %w: %w", err, pollError)
	}

	return nil
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

	if err := c.tracker.saveOrMerge(eps); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncHandler failed to reconcile resource key: %s", key))
		return err
	}

	return c.reconcile(ctx)
}

// reconcile compares the actual state with the desired, and attempts to converge the two.
func (c *Controller) reconcile(ctx context.Context) error {
	hashrings, owners := c.generate(c.tracker.deepCopyState())

	hashringConfig, err := json.Marshal(hashrings)
	if err != nil {
		return err
	}
	// Get the ConfigMap or create it if it doesn't exist.
	cm, err := c.configMapLister.ConfigMaps(c.namespace).Get(c.configMapName)
	if errors.IsNotFound(err) {
		cm, err = c.client.CoreV1().ConfigMaps(c.namespace).Create(ctx,
			c.newConfigMap(string(hashringConfig), owners), metav1.CreateOptions{})
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	if cm.Data[c.configMapKey] != string(hashringConfig) {
		_, err = c.client.CoreV1().ConfigMaps(c.namespace).Update(
			ctx, c.newConfigMap(string(hashringConfig), owners), metav1.UpdateOptions{})
	}

	// If an error occurs during Update, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	c.metrics.configMapLastChangeSuccessTime.Set(float64(time.Now().Unix()))
	c.metrics.configMapLastChangeSuccessTime.Set(hashAsMetricValue(hashringConfig))
	return nil
}

// generate outputs the sorted hashrings that are currently in the cache.
// It does not look at TTLs and takes anything that is in the cache
// as the source of truth. It is read only and safe to call concurrently.
func (c *Controller) generate(state map[cacheKey]ownerRefTracker) (config.Hashrings, []metav1.OwnerReference) {
	var owners []metav1.OwnerReference
	generated := config.Hashrings{}

	for cacheKey, ownerRefs := range state {
		hashringName, svcName := c.tracker.unwrapCacheKey(cacheKey)

		for nameAndUID, _ := range ownerRefs {
			owners = append(owners, c.buildOwnerReference(nameAndUID))
		}

		sortedTenants, sortedEndpoints := c.deduplicateAndSortByOwnerRef(svcName, ownerRefs)

		generated = append(generated, config.Hashring{
			HashringSpec: config.HashringSpec{
				Name:    hashringName,
				Tenants: sortedTenants,
			},
			Endpoints: sortedEndpoints,
		})
	}

	sort.Sort(generated)
	sort.Slice(owners, func(i, j int) bool {
		return owners[i].Name < owners[j].Name
	})

	return generated, owners
}

// newConfigMap creates a new ConfigMap
// It sets a label so that the controller can watch for changes to the ConfigMap
func (c *Controller) newConfigMap(data string, owners []metav1.OwnerReference) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.configMapName,
			Namespace: c.namespace,
			Labels: map[string]string{
				ConfigMapLabel: "true",
			},
			OwnerReferences: owners,
		},
		Data: map[string]string{
			DefaultConfigMapKey: data,
		},
		BinaryData: nil,
	}
}

func (c *Controller) buildOwnerReference(key ownerRefUID) metav1.OwnerReference {
	name, uid := c.tracker.fromSubKey(key)

	return metav1.OwnerReference{
		APIVersion: discoveryv1.SchemeGroupVersion.String(),
		Kind:       "EndpointSlice",
		Name:       name,
		UID:        uid,
	}
}

// deduplicateAndSortByOwnerRef takes the overarching Service owner and a map of ownerRefs and
// returns a deduplicated and sorted list of tenants and endpoints
func (c *Controller) deduplicateAndSortByOwnerRef(svc string, ownerRefs ownerRefTracker) (tenants, endpoints []string) {
	aggregatedTenants := make(map[string]struct{})
	aggregatedEndpoints := make(map[string]struct{})

	for _, data := range ownerRefs {
		for _, tenant := range data.tenants {
			aggregatedTenants[tenant] = struct{}{}
		}
		for endpoint, _ := range data.endpoints {
			aggregatedEndpoints[endpoint] = struct{}{}
		}
	}

	endpoints = make([]string, 0, len(aggregatedEndpoints))
	for endpoint, _ := range aggregatedEndpoints {
		endpoints = append(endpoints, c.buildFQDN(endpoint, svc))
	}

	tenants = make([]string, 0, len(aggregatedTenants))
	for tenant, _ := range aggregatedTenants {
		tenants = append(tenants, tenant)
	}

	sort.Strings(endpoints)
	sort.Strings(tenants)
	return tenants, endpoints
}

// defaultBuildFQDN builds the FQDN for a given hostname
// https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-network-id
func (c *Controller) defaultBuildFQDN(hostname, serviceName string) string {
	return fmt.Sprintf("%s.%s.%s.svc.%s:%s",
		hostname, serviceName, c.namespace, defaultClusterDomain, defaultPort)
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

func buildOpts(o *Options) *Options {
	if o == nil {
		o = &Options{
			TTL:           nil,
			ConfigMapName: pointer.String(DefaultConfigMapName),
			ConfigMapKey:  pointer.String(DefaultConfigMapKey),
			Port:          pointer.String(defaultPort),
			ClusterDomain: pointer.String(defaultClusterDomain),
		}
		return o
	}

	if o.ConfigMapName == nil {
		o.ConfigMapName = pointer.String(DefaultConfigMapName)
	}
	if o.ConfigMapKey == nil {
		o.ConfigMapKey = pointer.String(DefaultConfigMapKey)
	}
	if o.Port == nil {
		o.Port = pointer.String(defaultPort)
	}
	if o.ClusterDomain == nil {
		o.ClusterDomain = pointer.String(defaultClusterDomain)
	}

	return o
}
