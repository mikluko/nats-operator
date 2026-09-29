package fixtures

import (
	"os"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// MetricsTLS writes to path, owner-readable, a kubernetes.io/tls Secret
// named name in namespace, holding a new CA as ca.crt and the certificate and
// key it signs for dnsNames.
func MetricsTLS(path, name, namespace string, dnsNames []string) error {
	ca, cert, key, err := certificates(name+"-ca", dnsNames)
	if err != nil {
		return err
	}
	raw, err := yaml.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"type":       string(corev1.SecretTypeTLS),
		"stringData": map[string]string{"ca.crt": ca, corev1.TLSCertKey: cert, corev1.TLSPrivateKeyKey: key},
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path, append([]byte(generated), raw...), 0o600)
}
