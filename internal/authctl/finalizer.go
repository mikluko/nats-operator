package authctl

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// patchFinalizer adds finalizer to obj, or removes it when add is false,
// and persists that change alone, where obj needs it. An update of the
// whole object would write spec as the Go types encode it, which can differ
// from what was applied (48h becomes 48h0m0s) and move the generation.
func patchFinalizer(ctx context.Context, c client.Client, obj client.Object, finalizer string, add bool) error {
	base, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("copy %T", obj)
	}
	var changed bool
	if add {
		changed = controllerutil.AddFinalizer(obj, finalizer)
	} else {
		changed = controllerutil.RemoveFinalizer(obj, finalizer)
	}
	if !changed {
		return nil
	}
	if err := c.Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("patch finalizers: %w", err)
	}
	return nil
}
