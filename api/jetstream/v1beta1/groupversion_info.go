package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion identifies this API group and version.
var GroupVersion = schema.GroupVersion{Group: "jetstream.nats.mikluko.io", Version: "v1beta1"}

// SchemeBuilder registers this group's kinds with a scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
})

// AddToScheme adds this group's kinds to a scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func register(objs ...runtime.Object) {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, objs...)
		return nil
	})
}
