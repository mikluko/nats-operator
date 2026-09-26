package lifecycle

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// FieldOwner is the field manager the JetStream controller writes specs,
// statuses and finalizers as.
const FieldOwner = "jetstream-controller"

// AddFinalizer adds Finalizer to obj and persists it, where obj lacks it.
func AddFinalizer(ctx context.Context, c client.Client, obj client.Object) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok || !controllerutil.AddFinalizer(obj, Finalizer) {
		return nil
	}
	if err := c.Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
		return fmt.Errorf("add finalizer: %w", err)
	}
	return nil
}

// RemoveFinalizer removes Finalizer from obj and persists it, where obj
// carries it; obj being gone already is not an error.
func RemoveFinalizer(ctx context.Context, c client.Client, obj client.Object) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok || !controllerutil.RemoveFinalizer(obj, Finalizer) {
		return nil
	}
	if err := c.Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
		return client.IgnoreNotFound(fmt.Errorf("remove finalizer: %w", err))
	}
	return nil
}

// SpecOrDeletion passes creates, deletes, and updates that change the
// generation or mark the object for deletion, so a controller's own status
// writes do not requeue it.
func SpecOrDeletion() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() ||
				(e.ObjectOld.GetDeletionTimestamp() == nil && e.ObjectNew.GetDeletionTimestamp() != nil)
		},
	}
}

// PatchSpec persists want, a modified copy of obj, with an optimistic lock
// where newSpec, want's spec, differs from *spec, obj's; it then sets *spec
// to newSpec and obj's resource version and generation to what was stored,
// leaving obj's status alone.
func PatchSpec[S any](ctx context.Context, c client.Client, obj, want client.Object, spec *S, newSpec S) error {
	if equality.Semantic.DeepEqual(*spec, newSpec) {
		return nil
	}
	if err := c.Patch(ctx, want, client.MergeFromWithOptions(obj, client.MergeFromWithOptimisticLock{}), client.FieldOwner(FieldOwner)); err != nil {
		return fmt.Errorf("write spec: %w", err)
	}
	*spec = newSpec
	obj.SetResourceVersion(want.GetResourceVersion())
	obj.SetGeneration(want.GetGeneration())
	return nil
}

// PatchStatus writes obj's status over base's where cur, obj's status,
// differs from old, base's; obj being gone is not an error.
func PatchStatus[S any](ctx context.Context, c client.Client, base, obj client.Object, old, cur S) error {
	if equality.Semantic.DeepEqual(old, cur) {
		return nil
	}
	if err := c.Status().Patch(ctx, obj, client.MergeFrom(base), client.FieldOwner(FieldOwner)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}
