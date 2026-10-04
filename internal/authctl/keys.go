package authctl

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nkeys"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// SeedKey is the key a generated seed Secret holds its seed under.
const SeedKey = "seed"

// GeneratedForAnnotation marks a generated seed Secret with the object it
// was generated for, as "<operator|systemaccount|account>/<name>". The
// Secret carries no owner reference: it outlives that object, and an object
// of the same kind and name applied again reads it.
const GeneratedForAnnotation = "auth.nats-operator.io/generated-for"

// generatedSigningKeyName is the name of the signing key generated where
// spec lists none.
const generatedSigningKeyName = "generated"

// errKeysPending is returned when a seed Secret does not exist, or holds no
// seed under the key named, and the caller may not create it.
var errKeysPending = errors.New("seed not present")

// errInvalidSeed is returned for a seed that does not decode, or is not of
// the kind its role requires.
var errInvalidSeed = errors.New("invalid seed")

// errSeedLost is returned for a generated identity seed Secret that is
// absent while its owner's status records the identity.
var errSeedLost = errors.New("identity seed lost")

// errSeedNotOwned is returned for a seed Secret under a generated name that
// is not annotated as generated for its owner.
var errSeedNotOwned = errors.New("seed Secret not generated for its owner")

// Roles name the kind of identity a generated seed Secret belongs to. No
// role is a hyphen-delimited suffix of another, so generatedSecretName
// gives no two owners in a namespace the same Secret.
const (
	roleOperator      = "operator"
	roleSystemAccount = "systemaccount"
	roleAccount       = "account"
)

// keySource is where a NATS operator's or account's keys come from.
type keySource struct {
	// owner is the NatsOperator, NatsSystemAccount or NatsAccount; its
	// namespace is where every seed Secret is read.
	owner client.Object
	keys  *authv1beta1.Keys
	// publicKey is an identity held offline; set, no identity seed is read
	// or generated.
	publicKey string
	// recorded is the identity the owner's status holds; set, a missing
	// generated identity seed is not generated again.
	recorded string
	prefix   nkeys.PrefixByte
	role     string
}

type resolvedKeys struct {
	jwtplane.Keys
	// Generated names the Secrets the auth controller generated seeds into.
	Generated authv1beta1.SeedSecrets
}

func (k resolvedKeys) identityPublicKey() (string, error) {
	if k.Identity == nil {
		return k.PublicKey, nil
	}
	return k.Identity.PublicKey()
}

// signingPublicKeys returns every signing key's public key, and those of
// the retiring ones apart.
func (k resolvedKeys) signingPublicKeys() (all, retiring []string, err error) {
	for _, sk := range k.Signing {
		pub, err := sk.Pair.PublicKey()
		if err != nil {
			return nil, nil, err
		}
		all = append(all, pub)
		if sk.Retiring {
			retiring = append(retiring, pub)
		}
	}
	return all, retiring, nil
}

func generatedSecretName(owner, role, key string) string {
	return owner + "-" + role + "-" + key
}

// resolveKeys reads the keys src names, and a key spec leaves out from the
// Secret generated for it, created only with generate and otherwise wrapping
// errKeysPending. A generated identity seed missing while src records an
// identity wraps errSeedLost and is never created.
func resolveKeys(ctx context.Context, c client.Client, src keySource, generate bool) (resolvedKeys, error) {
	out, err := resolveIdentity(ctx, c, src, generate)
	if err != nil {
		return out, err
	}
	ns := src.owner.GetNamespace()
	if src.keys != nil && len(src.keys.Signing) > 0 {
		for _, sk := range src.keys.Signing {
			kp, err := readSeed(ctx, c, ns, sk.SecretKeyRef.Name, sk.SecretKeyRef.Key, src.prefix)
			if err != nil {
				return out, fmt.Errorf("signing key %q: %w", sk.Name, err)
			}
			out.Signing = append(out.Signing, jwtplane.SigningKey{Name: sk.Name, Pair: kp, Retiring: sk.Retiring})
		}
		return out, nil
	}
	name := generatedSecretName(src.owner.GetName(), src.role, "signing-1")
	kp, err := generatedSeed(ctx, c, src.owner, src.role, name, src.prefix, generate)
	if err != nil {
		return out, fmt.Errorf("signing key: %w", err)
	}
	out.Signing = []jwtplane.SigningKey{{Name: generatedSigningKeyName, Pair: kp}}
	out.Generated.Signing = []string{name}
	return out, nil
}

// resolveIdentity is resolveKeys for the identity alone.
func resolveIdentity(ctx context.Context, c client.Client, src keySource, generate bool) (resolvedKeys, error) {
	var out resolvedKeys
	switch {
	case src.keys != nil && src.keys.Identity != nil:
		kp, err := readSeed(ctx, c, src.owner.GetNamespace(), src.keys.Identity.SecretKeyRef.Name, src.keys.Identity.SecretKeyRef.Key, src.prefix)
		if err != nil {
			return out, fmt.Errorf("identity key: %w", err)
		}
		out.Identity = kp
	case src.publicKey != "":
		out.PublicKey = src.publicKey
	default:
		name := generatedSecretName(src.owner.GetName(), src.role, "identity")
		kp, err := generatedSeed(ctx, c, src.owner, src.role, name, src.prefix, generate && src.recorded == "")
		if src.recorded != "" && errors.Is(err, errKeysPending) {
			return out, fmt.Errorf("%w: %s is recorded as the identity, and generating another would orphan every JWT issued under it; restore the seed into it or name it in keys.identity: %w",
				errSeedLost, src.recorded, err)
		}
		if err != nil {
			return out, fmt.Errorf("identity key: %w", err)
		}
		out.Identity = kp
		out.Generated.Identity = name
	}
	return out, nil
}

func readSeed(ctx context.Context, c client.Reader, namespace, name, key string, prefix nkeys.PrefixByte) (nkeys.KeyPair, error) {
	s, err := getSeedSecret(ctx, c, namespace, name)
	if err != nil {
		return nil, err
	}
	return seedFrom(s, key, prefix)
}

// getSeedSecret gets Secret namespace/name; a missing one wraps
// errKeysPending.
func getSeedSecret(ctx context.Context, c client.Reader, namespace, name string) (*corev1.Secret, error) {
	var s corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &s); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: Secret %s/%s does not exist", errKeysPending, namespace, name)
		}
		return nil, fmt.Errorf("get Secret %s/%s: %w", namespace, name, err)
	}
	return &s, nil
}

// seedFrom parses the seed under key in s; a missing key wraps
// errKeysPending, a seed of the wrong kind errInvalidSeed.
func seedFrom(s *corev1.Secret, key string, prefix nkeys.PrefixByte) (nkeys.KeyPair, error) {
	seed, ok := s.Data[key]
	if !ok {
		return nil, fmt.Errorf("%w: Secret %s/%s has no key %q", errKeysPending, s.Namespace, s.Name, key)
	}
	kp, err := jwtplane.ParseSeed(seed, prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: Secret %s/%s key %q: %w", errInvalidSeed, s.Namespace, s.Name, key, err)
	}
	return kp, nil
}

// generatedSeed reads the generated seed Secret name in owner's namespace,
// creating it for owner, of role, when generate is set and it is absent. A
// Secret not annotated as generated for owner wraps errSeedNotOwned.
func generatedSeed(ctx context.Context, c client.Client, owner client.Object, role, name string, prefix nkeys.PrefixByte, generate bool) (nkeys.KeyPair, error) {
	generatedFor := role + "/" + owner.GetName()
	existing, err := getSeedSecret(ctx, c, owner.GetNamespace(), name)
	if err == nil {
		if existing.Annotations[GeneratedForAnnotation] != generatedFor {
			return nil, fmt.Errorf("%w: Secret %s/%s is not annotated %s: %s", errSeedNotOwned, owner.GetNamespace(), name, GeneratedForAnnotation, generatedFor)
		}
		return seedFrom(existing, SeedKey, prefix)
	}
	if !errors.Is(err, errKeysPending) || !generate {
		return nil, err
	}
	seed, err := jwtplane.GenerateSeed(prefix)
	if err != nil {
		return nil, err
	}
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   owner.GetNamespace(),
			Name:        name,
			Annotations: map[string]string{GeneratedForAnnotation: generatedFor},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{SeedKey: seed},
	}
	if err := c.Create(ctx, s); err != nil {
		return nil, fmt.Errorf("create Secret %s/%s: %w", s.Namespace, name, err)
	}
	return jwtplane.ParseSeed(seed, prefix)
}

// operatorKeySource is where op's keys come from; a NATS operator JWT signed
// offline names the identity.
func operatorKeySource(op *authv1beta1.NatsOperator) (keySource, error) {
	src := keySource{owner: op, keys: op.Spec.Keys, recorded: op.Status.PublicKey, prefix: nkeys.PrefixByteOperator, role: roleOperator}
	if op.Spec.JWT != "" {
		pub, err := offlineOperatorSubject(op.Spec.JWT)
		if err != nil {
			return src, err
		}
		src.publicKey = pub
	}
	return src, nil
}

func systemAccountKeySource(sys *authv1beta1.NatsSystemAccount) keySource {
	return keySource{owner: sys, keys: sys.Spec.Keys, publicKey: sys.Spec.PublicKey, recorded: sys.Status.PublicKey, prefix: nkeys.PrefixByteAccount, role: roleSystemAccount}
}

func accountKeySource(acc *authv1beta1.NatsAccount) keySource {
	return keySource{owner: acc, keys: acc.Spec.Keys, publicKey: acc.Spec.PublicKey, recorded: acc.Status.PublicKey, prefix: nkeys.PrefixByteAccount, role: roleAccount}
}

// seedSecretNames lists the Secrets src reads its seeds from, generated
// ones included.
func seedSecretNames(src keySource) []string {
	var out []string
	switch {
	case src.keys != nil && src.keys.Identity != nil:
		out = append(out, src.keys.Identity.SecretKeyRef.Name)
	case src.publicKey == "":
		out = append(out, generatedSecretName(src.owner.GetName(), src.role, "identity"))
	}
	if src.keys == nil || len(src.keys.Signing) == 0 {
		return append(out, generatedSecretName(src.owner.GetName(), src.role, "signing-1"))
	}
	for _, sk := range src.keys.Signing {
		out = append(out, sk.SecretKeyRef.Name)
	}
	return out
}
