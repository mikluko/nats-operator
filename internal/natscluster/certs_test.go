package natscluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/sysobs"
)

func TestCertsLoaded(t *testing.T) {
	a := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	b := a.Add(time.Hour)
	for _, tt := range []struct {
		name  string
		certs Certs
		got   sysobs.CertNotAfter
		want  bool
	}{
		{"no TLS", Certs{}, sysobs.CertNotAfter{}, true},
		{"same", Certs{Routes: MountedCert{NotAfter: a}, Gateway: MountedCert{NotAfter: b}}, sysobs.CertNotAfter{Cluster: a, Gateway: b}, true},
		{"routes behind", Certs{Routes: MountedCert{NotAfter: b}}, sysobs.CertNotAfter{Cluster: a}, false},
		{"leafnodes behind", Certs{Leafnodes: MountedCert{NotAfter: b}}, sysobs.CertNotAfter{Leafnode: a}, false},
		{"unreported", Certs{Gateway: MountedCert{NotAfter: b}}, sysobs.CertNotAfter{}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.certs.loaded(tt.got))
		})
	}
}
