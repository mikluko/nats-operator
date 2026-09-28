package secretreads

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	const (
		metaList  = "application/vnd.kubernetes.protobuf;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json"
		metaWatch = "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json"
		whole     = "application/vnd.kubernetes.protobuf,application/json"
	)
	tests := []struct {
		name     string
		method   string
		target   string
		accept   string
		wantMeta int
		wantFull bool
	}{
		{name: "metadata list, cluster", method: http.MethodGet, target: "/api/v1/secrets?limit=500", accept: metaList, wantMeta: 1},
		{name: "metadata watch, namespace", method: http.MethodGet, target: "/api/v1/namespaces/ns/secrets?watch=true", accept: metaWatch, wantMeta: 1},
		{name: "whole list", method: http.MethodGet, target: "/api/v1/secrets", accept: whole, wantFull: true},
		{name: "whole watch, namespace", method: http.MethodGet, target: "/api/v1/namespaces/ns/secrets?watch=true", accept: whole, wantFull: true},
		{name: "no accept", method: http.MethodGet, target: "/api/v1/secrets", wantFull: true},
		{name: "one secret", method: http.MethodGet, target: "/api/v1/namespaces/ns/secrets/creds", accept: whole},
		{name: "create", method: http.MethodPost, target: "/api/v1/namespaces/ns/secrets", accept: whole},
		{name: "other kind", method: http.MethodGet, target: "/api/v1/configmaps", accept: whole},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reads{}
			req := httptest.NewRequest(tt.method, tt.target, nil)
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}
			r.record(req)
			require.Equal(t, tt.wantMeta, r.meta)
			require.Equal(t, tt.wantFull, len(r.full) > 0)
		})
	}
}
