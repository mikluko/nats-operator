package jsapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(5*time.Second))
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestRequest(t *testing.T) {
	nc := connect(t)
	replies := map[string]string{
		"ok":               `{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{}}`,
		"stream-missing":   `{"error":{"code":404,"err_code":10059,"description":"stream not found"}}`,
		"consumer-missing": `{"error":{"code":404,"err_code":10014,"description":"consumer not found"}}`,
		"no-move":          `{"error":{"code":400,"err_code":10129,"description":"stream move not in progress"}}`,
	}
	for subject, reply := range replies {
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) { _ = m.Respond([]byte(reply)) })
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	for _, tc := range []struct {
		subject  string
		code     jetstream.ErrorCode
		notFound bool
	}{
		{subject: "ok"},
		{subject: "stream-missing", code: jetstream.JSErrCodeStreamNotFound, notFound: true},
		{subject: "consumer-missing", code: jetstream.JSErrCodeConsumerNotFound, notFound: true},
		{subject: "no-move", code: 10129},
	} {
		t.Run(tc.subject, func(t *testing.T) {
			data, err := Request(context.Background(), nc, tc.subject, struct{}{})
			if tc.code == 0 {
				require.NoError(t, err)
				require.JSONEq(t, replies[tc.subject], string(data))
				return
			}
			var apiErr *jetstream.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tc.code, apiErr.ErrorCode)
			require.Equal(t, tc.notFound, errors.Is(err, ErrNotFound))
		})
	}
}
