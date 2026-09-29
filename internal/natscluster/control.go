package natscluster

import (
	"context"
	"errors"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// notControlledError refuses a write to objects that exist and are not
// controlled by the NatsCluster being reconciled.
type notControlledError struct {
	// Objects are "<Kind> <name>", in the order they were refused.
	Objects []string
}

func (e *notControlledError) Error() string {
	return "not controlled by this NatsCluster: " + strings.Join(e.Objects, ", ")
}

// notControlled returns the refusal of a write to obj.
func (r *Reconciler) notControlled(obj client.Object) *notControlledError {
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if gvk, err := apiutil.GVKForObject(obj, r.Client.Scheme()); err == nil {
		kind = gvk.Kind
	}
	return &notControlledError{Objects: []string{kind + " " + obj.GetName()}}
}

// createOrUpdate creates obj or updates it with mutate, controlled by nc. It
// returns a *notControlledError, writing nothing, when obj exists and nc
// does not control it.
func (r *Reconciler) createOrUpdate(ctx context.Context, nc *clusterv1beta1.NatsCluster, obj client.Object, mutate func()) error {
	apply := func() error {
		if obj.GetResourceVersion() != "" && !metav1.IsControlledBy(obj, nc) {
			return r.notControlled(obj)
		}
		mutate()
		return controllerutil.SetControllerReference(nc, obj, r.Client.Scheme())
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, apply)
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	if err := r.readExisting(ctx, nc, obj); err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	return r.Client.Update(ctx, obj)
}

// refusals accumulates the objects several writes refused.
type refusals struct {
	objects []string
}

// add records err's objects and returns nil when err is a
// *notControlledError, and returns err otherwise.
func (f *refusals) add(err error) error {
	var nce *notControlledError
	if !errors.As(err, &nce) {
		return err
	}
	f.objects = append(f.objects, nce.Objects...)
	return nil
}

// err is every refusal added, nil when there was none.
func (f *refusals) err() error {
	if len(f.objects) == 0 {
		return nil
	}
	return &notControlledError{Objects: f.objects}
}
