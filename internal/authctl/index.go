package authctl

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// Field indexes, each valued "<namespace>/<name>" of the object named.
const (
	// operatorField indexes NatsSystemAccount, NatsAccount and
	// NatsOperatorTrust by the NatsOperator they name.
	operatorField = "auth.nats.mikluko.io/operator"
	// systemAccountField indexes NatsOperator by its NatsSystemAccount.
	systemAccountField = "auth.nats.mikluko.io/system-account"
	// exporterField indexes NatsAccount by the NatsAccounts it imports
	// from.
	exporterField = "auth.nats.mikluko.io/exporter"
	// accountField indexes NatsAccountTrust by its NatsAccount.
	accountField = "auth.nats.mikluko.io/account"
	// seedSecretField indexes NatsOperator, NatsSystemAccount and
	// NatsAccount by the seed Secrets they read, generated ones included.
	seedSecretField = "auth.nats.mikluko.io/seed-secret"
	// userAccountField indexes NatsUser by its account, valued by
	// accountValue.
	userAccountField = "auth.nats.mikluko.io/user-account"
)

// accountValue is the userAccountField value of the account of kind at k.
func accountValue(kind authv1beta1.AccountKind, k types.NamespacedName) string {
	return string(kind) + ":" + keyValue(k)
}

// refKey returns ref's key, in namespace when ref names none.
func refKey(ref natsv1beta1.ObjectReference, namespace string) types.NamespacedName {
	if ref.Namespace != "" {
		namespace = ref.Namespace
	}
	return types.NamespacedName{Namespace: namespace, Name: ref.Name}
}

func keyValue(k types.NamespacedName) string { return k.String() }

// indexes registers every field index the reconcilers list by.
func indexes(ctx context.Context, idx client.FieldIndexer) error {
	type index struct {
		obj   client.Object
		field string
		fn    client.IndexerFunc
	}
	nsOf := func(o client.Object, ref natsv1beta1.ObjectReference) []string {
		return []string{keyValue(refKey(ref, o.GetNamespace()))}
	}
	secrets := func(o client.Object, src keySource) []string {
		var out []string
		for _, name := range seedSecretNames(src) {
			out = append(out, keyValue(types.NamespacedName{Namespace: o.GetNamespace(), Name: name}))
		}
		return out
	}
	all := []index{
		{&authv1beta1.NatsSystemAccount{}, operatorField, func(o client.Object) []string {
			return nsOf(o, o.(*authv1beta1.NatsSystemAccount).Spec.OperatorRef)
		}},
		{&authv1beta1.NatsAccount{}, operatorField, func(o client.Object) []string {
			return nsOf(o, o.(*authv1beta1.NatsAccount).Spec.OperatorRef)
		}},
		{&natsv1beta1.NatsOperatorTrust{}, operatorField, func(o client.Object) []string {
			ref := o.(*natsv1beta1.NatsOperatorTrust).Spec.OperatorRef
			if ref == nil {
				return nil
			}
			return nsOf(o, *ref)
		}},
		{&natsv1beta1.NatsAccountTrust{}, accountField, func(o client.Object) []string {
			ref := o.(*natsv1beta1.NatsAccountTrust).Spec.AccountRef
			if ref == nil {
				return nil
			}
			return nsOf(o, *ref)
		}},
		{&authv1beta1.NatsOperator{}, systemAccountField, func(o client.Object) []string {
			return nsOf(o, o.(*authv1beta1.NatsOperator).Spec.SystemAccountRef)
		}},
		{&authv1beta1.NatsAccount{}, exporterField, func(o client.Object) []string {
			var out []string
			for _, imp := range o.(*authv1beta1.NatsAccount).Spec.Imports {
				if imp.AccountRef.Kind == authv1beta1.AccountKindAccount {
					out = append(out, nsOf(o, imp.AccountRef.ObjectReference)...)
				}
			}
			return out
		}},
		{&authv1beta1.NatsUser{}, userAccountField, func(o client.Object) []string {
			ref := o.(*authv1beta1.NatsUser).Spec.AccountRef
			return []string{accountValue(ref.Kind, refKey(ref.ObjectReference, o.GetNamespace()))}
		}},
		{&authv1beta1.NatsOperator{}, seedSecretField, func(o client.Object) []string {
			src, _ := operatorKeySource(o.(*authv1beta1.NatsOperator))
			return secrets(o, src)
		}},
		{&authv1beta1.NatsSystemAccount{}, seedSecretField, func(o client.Object) []string {
			return secrets(o, systemAccountKeySource(o.(*authv1beta1.NatsSystemAccount)))
		}},
		{&authv1beta1.NatsAccount{}, seedSecretField, func(o client.Object) []string {
			return secrets(o, accountKeySource(o.(*authv1beta1.NatsAccount)))
		}},
	}
	for _, i := range all {
		if err := idx.IndexField(ctx, i.obj, i.field, i.fn); err != nil {
			return err
		}
	}
	grantTargets := []struct {
		obj     client.Object
		targets func(client.Object) []string
	}{
		{&authv1beta1.NatsOperator{}, func(o client.Object) []string {
			return []string{o.(*authv1beta1.NatsOperator).Spec.SystemAccountRef.Namespace}
		}},
		{&authv1beta1.NatsSystemAccount{}, func(o client.Object) []string {
			return []string{o.(*authv1beta1.NatsSystemAccount).Spec.OperatorRef.Namespace}
		}},
		{&authv1beta1.NatsAccount{}, func(o client.Object) []string {
			acc := o.(*authv1beta1.NatsAccount)
			out := []string{acc.Spec.OperatorRef.Namespace}
			for _, imp := range acc.Spec.Imports {
				out = append(out, imp.AccountRef.Namespace)
			}
			return out
		}},
		{&authv1beta1.NatsUser{}, func(o client.Object) []string {
			return []string{o.(*authv1beta1.NatsUser).Spec.AccountRef.Namespace}
		}},
		{&natsv1beta1.NatsOperatorTrust{}, func(o client.Object) []string {
			if ref := o.(*natsv1beta1.NatsOperatorTrust).Spec.OperatorRef; ref != nil {
				return []string{ref.Namespace}
			}
			return nil
		}},
		{&natsv1beta1.NatsAccountTrust{}, func(o client.Object) []string {
			if ref := o.(*natsv1beta1.NatsAccountTrust).Spec.AccountRef; ref != nil {
				return []string{ref.Namespace}
			}
			return nil
		}},
	}
	for _, g := range grantTargets {
		if err := grant.IndexReferrers(ctx, idx, g.obj, g.targets); err != nil {
			return err
		}
	}
	return nil
}
