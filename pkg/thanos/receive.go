package thanos

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/philipgough/hashring-controller/pkg/controller"

	discoveryv1 "k8s.io/api/discovery/v1"
)

// HashringAlgorithm is the algorithm used to distribute series in the ring.
type HashringAlgorithm string

const (
	AlgorithmHashmod HashringAlgorithm = "hashmod"
	AlgorithmKetama  HashringAlgorithm = "ketama"
)

// HashringConfig represents the configuration for a hashring
// a receive node knows about.
type HashringConfig struct {
	Hashring       string            `json:"hashring,omitempty"`
	Tenants        []string          `json:"tenants"`
	Endpoints      []string          `json:"endpoints"`
	Algorithm      HashringAlgorithm `json:"algorithm,omitempty"`
	ExternalLabels map[string]string `json:"external_labels,omitempty"`
}

type Hashrings []*HashringConfig

func (h Hashrings) Len() int {
	return len(h)
}

func (h Hashrings) Less(i, j int) bool {
	return h[i].Hashring < h[j].Hashring
}

func (h Hashrings) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

const (
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

	// defaultPort is the default port used by the Thanos receive component.
	defaultPort = 10901
)

// DefaultEndpointBuilder builds the endpoint string for a given Endpoint.
// This implementation assumes endpoints are backed by a Service within the same namespace.
// https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-network-id
func DefaultEndpointBuilder(eps *discoveryv1.EndpointSlice, ep discoveryv1.Endpoint) (string, error) {
	if ep.Hostname == nil || *ep.Hostname == "" {
		return "", fmt.Errorf("missing required hostname on Endpoint")
	}
	hostname := *ep.Hostname

	svc, hasSvc := svcFromEndpointSlice(eps)
	if !hasSvc {
		return "", fmt.Errorf("missing Service name label on EndpointSlice %s/%s", eps.Namespace, eps.Name)
	}

	return fmt.Sprintf("%s.%s:%d", hostname, svc, defaultPort), nil
}

// DefaultConfig returns a default configuration for the ReceiveHashringGenerator.
func DefaultConfig() *Config {
	return &Config{
		HashringTenants:             nil,
		EndpointBuilder:             DefaultEndpointBuilder,
		HashringNameIdentifierLabel: HashringNameIdentifierLabel,
		TenantListIdentifierLabel:   TenantIdentifierLabel,
	}
}

// Config is the configuration for the ReceiveHashringGenerator.
type Config struct {
	// HashringTenants is an optional map of hashring name to a list of tenants.
	// If present, this takes precedence over the tenants in the EndpointSlice label.
	HashringTenants             map[string][]string
	EndpointBuilder             controller.EndpointBuilderFN
	HashringNameIdentifierLabel string
	TenantListIdentifierLabel   string
}

// ReceiveHashringGenerator is an implementation of controller.Trackable that generates a Thanos hashring configuration
// for the Thanos receive component.
type ReceiveHashringGenerator struct {
	conf   *Config
	logger *slog.Logger
}

// NewReceiveHashringController returns a new ReceiveHashringGenerator.
func NewReceiveHashringController(logger *slog.Logger, config *Config) *ReceiveHashringGenerator {
	if logger == nil {
		logger = slog.Default()
	}

	if config == nil {
		config = DefaultConfig()
	}

	return &ReceiveHashringGenerator{
		logger: logger,
		conf:   config,
	}
}

// EndpointBuilder is a function that builds the endpoint string for a given Endpoint.
// Returns the function from Config.EndpointBuilder if present, otherwise
// returns the default implementation DefaultEndpointBuilder.
func (r *ReceiveHashringGenerator) EndpointBuilder() controller.EndpointBuilderFN {
	if r.conf.EndpointBuilder != nil {
		return r.conf.EndpointBuilder
	}
	return DefaultEndpointBuilder
}

// SetValueFor the Thanos receive hashring configuration for a given EndpointSlice in the cache.
func (r *ReceiveHashringGenerator) SetValueFor(eps *discoveryv1.EndpointSlice, endpoints []string) (interface{}, error) {
	svc := eps.GetLabels()[discoveryv1.LabelServiceName]
	if svc == "" {
		return "", fmt.Errorf("missing Service name label on EndpointSlice %s/%s", eps.Namespace, eps.Name)
	}

	ns := eps.GetNamespace()
	if ns == "" {
		return "", fmt.Errorf("missing namespace on EndpointSlice %s/%s", eps.Namespace, eps.Name)
	}

	return &HashringConfig{
		Hashring:  r.getHashringName(eps),
		Tenants:   r.getTenants(eps),
		Endpoints: endpoints,
	}, nil
}

func (r *ReceiveHashringGenerator) Generate(values [][]interface{}) (string, error) {
	var deduplicatedAndSortedHashrings Hashrings

	for _, value := range values {
		var hashrings Hashrings
		for _, v := range value {
			hashring, ok := v.(*HashringConfig)
			if !ok {
				return "", fmt.Errorf("expected HashringConfig but got %T", v)
			}
			hashrings = append(hashrings, hashring)
		}
		deduplicatedAndSortedHashrings = append(deduplicatedAndSortedHashrings, deduplicateAndSortHashrings(hashrings))
	}

	sort.Sort(deduplicatedAndSortedHashrings)
	b, err := json.Marshal(deduplicatedAndSortedHashrings)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func deduplicateAndSortHashrings(hashrings []*HashringConfig) *HashringConfig {
	aggregatedTenants := make(map[string]struct{})
	aggregatedEndpoints := make(map[string]struct{})
	// names should be identical under the group
	var name string
	aggregatedNames := make(map[string]struct{})

	for _, hashring := range hashrings {
		for _, tenant := range hashring.Tenants {
			aggregatedTenants[tenant] = struct{}{}
		}
		for _, endpoint := range hashring.Endpoints {
			aggregatedEndpoints[endpoint] = struct{}{}
		}

		name = hashring.Hashring
		aggregatedNames[name] = struct{}{}
	}

	// ff this happens, we have a bug
	if len(aggregatedNames) != 1 {
		panic("hashring names are not identical")
	}

	endpoints := make([]string, 0, len(aggregatedEndpoints))
	for endpoint, _ := range aggregatedEndpoints {
		endpoints = append(endpoints, endpoint)
	}

	tenants := make([]string, 0, len(aggregatedTenants))
	for tenant, _ := range aggregatedTenants {
		tenants = append(tenants, tenant)
	}

	sort.Strings(tenants)
	sort.Strings(endpoints)

	return &HashringConfig{
		Hashring:  name,
		Tenants:   tenants,
		Endpoints: endpoints,
	}
}

func (r *ReceiveHashringGenerator) getHashringName(eps *discoveryv1.EndpointSlice) string {
	var name string
	name, ok := eps.GetLabels()[HashringNameIdentifierLabel]
	if ok && name != "" {
		return name
	}

	name, ok = svcFromEndpointSlice(eps)
	if !ok || name == "" {
		// this should never happen
		r.logger.Error("hashring name is empty", "endpointslice", eps.GetName())
	}

	return name
}

func (r *ReceiveHashringGenerator) getTenants(eps *discoveryv1.EndpointSlice) []string {
	// preconfigured tenants take precedence
	hashringName := r.getHashringName(eps)
	if preconfiguredTenants, ok := r.conf.HashringTenants[hashringName]; ok {
		return preconfiguredTenants
	}

	// otherwise we fall back to the tenant label
	tenant, ok := eps.GetLabels()[TenantIdentifierLabel]
	if ok {
		return []string{tenant}
	}

	// lastly we fall back to the soft tenancy for all tenants
	return []string{}
}

func svcFromEndpointSlice(eps *discoveryv1.EndpointSlice) (string, bool) {
	name, ok := eps.GetLabels()[discoveryv1.LabelServiceName]
	return name, ok
}
