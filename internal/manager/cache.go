package manager

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Owned is what a controller creates: every object of Kinds it creates
// carries Label. Kinds never holds Secret: the controllers watch Secrets they
// did not create.
type Owned struct {
	Label string
	Kinds []client.Object
}

// cacheOptions scopes a manager's cache: of each kind in owned.Kinds it holds
// only the objects carrying owned.Label, whatever its value; of every Secret,
// which the controllers watch metadata-only, it holds what secretMetadata
// keeps.
func cacheOptions(owned Owned) (cache.Options, error) {
	by := map[client.Object]cache.ByObject{
		&corev1.Secret{}: {Transform: secretMetadata},
	}
	if len(owned.Kinds) == 0 {
		return cache.Options{ByObject: by}, nil
	}
	req, err := labels.NewRequirement(owned.Label, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, fmt.Errorf("owner label: %w", err)
	}
	sel := labels.NewSelector().Add(*req)
	for _, k := range owned.Kinds {
		by[k] = cache.ByObject{Label: sel}
	}
	return cache.Options{ByObject: by}, nil
}

// ClientOptions makes a manager's client read Secrets from the API server,
// since a cached Get would start an informer over whole Secrets.
func ClientOptions() client.Options {
	return client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}}
}

// secretMetadata drops the annotations and managed fields of a Secret's
// metadata, since kubectl's last-applied configuration copies the data into
// its annotations; anything else passes through.
func secretMetadata(obj any) (any, error) {
	m, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return obj, nil
	}
	m.Annotations = nil
	m.ManagedFields = nil
	return m, nil
}
