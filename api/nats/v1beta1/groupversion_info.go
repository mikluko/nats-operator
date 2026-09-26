// Package v1beta1 is the nats.mikluko.io API group: the kinds every
// controller reads and none owns.
//
// +kubebuilder:object:generate=true
// +groupName=nats.mikluko.io
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion identifies this API group and version.
var GroupVersion = schema.GroupVersion{Group: "nats.mikluko.io", Version: "v1beta1"}

// SchemeBuilder registers this group's kinds with a scheme; each kind adds
// itself with SchemeBuilder.Register in its own file.
var SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
})

// AddToScheme adds this group's kinds to a scheme.
var AddToScheme = SchemeBuilder.AddToScheme
