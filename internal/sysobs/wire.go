package sysobs

import (
	"encoding/json"
	"slices"
	"time"
)

type wireServerInfo struct {
	Name      string            `json:"name"`
	ID        string            `json:"id"`
	Cluster   string            `json:"cluster"`
	Version   string            `json:"ver"`
	Metadata  map[string]string `json:"metadata"`
	JetStream bool              `json:"jetstream"`
	Tags      []string          `json:"tags"`
}

type wireError struct {
	Code        int    `json:"code"`
	Description string `json:"description"`
}

type wireFilter struct {
	Name       string `json:"server_name,omitempty"`
	Cluster    string `json:"cluster,omitempty"`
	ExactMatch bool   `json:"exact_match,omitempty"`
}

type wireStatsz struct {
	Server wireServerInfo `json:"server"`
	Stats  struct {
		Routes []struct {
			Name string `json:"name"`
		} `json:"routes"`
	} `json:"statsz"`
}

type wireJszRequest struct {
	wireFilter
	Accounts bool `json:"accounts,omitempty"`
	Streams  bool `json:"streams,omitempty"`
	Consumer bool `json:"consumer,omitempty"`
	Config   bool `json:"config,omitempty"`
	Offset   int  `json:"offset,omitempty"`
	Limit    int  `json:"limit,omitempty"`
}

type wireJszResponse struct {
	Server wireServerInfo `json:"server"`
	Data   *wireJSInfo    `json:"data"`
	Error  *wireError     `json:"error"`
}

type wireJSInfo struct {
	Meta     *wireMeta     `json:"meta_cluster"`
	Accounts []wireAccount `json:"account_details"`
	Total    int           `json:"total"`
}

type wireMeta struct {
	Leader   string     `json:"leader"`
	Replicas []wirePeer `json:"replicas"`
}

type wireAccount struct {
	Name    string       `json:"name"`
	ID      string       `json:"id"`
	Streams []wireStream `json:"stream_detail"`
}

type wireStream struct {
	Name      string            `json:"name"`
	Cluster   *wireCluster      `json:"cluster"`
	Config    *wireStreamConfig `json:"config"`
	Consumers []wireConsumer    `json:"consumer_detail"`
}

type wireStreamConfig struct {
	Placement *wirePlacement    `json:"placement"`
	Metadata  map[string]string `json:"metadata"`
}

type wirePlacement struct {
	Cluster string   `json:"cluster"`
	Tags    []string `json:"tags"`
}

type wireConsumer struct {
	Name    string       `json:"name"`
	Cluster *wireCluster `json:"cluster"`
}

type wireCluster struct {
	RaftGroup string     `json:"raft_group"`
	Leader    string     `json:"leader"`
	Replicas  []wirePeer `json:"replicas"`
}

type wirePeer struct {
	Name    string `json:"name"`
	Current bool   `json:"current"`
	Offline bool   `json:"offline"`
	Lag     uint64 `json:"lag"`
}

type wireVarzResponse struct {
	Data *struct {
		ConfigDigest    string          `json:"config_digest"`
		ConfigLoadTime  time.Time       `json:"config_load_time"`
		TLSCertNotAfter time.Time       `json:"tls_cert_not_after"`
		Cluster         wireListenerTLS `json:"cluster"`
		Gateway         wireListenerTLS `json:"gateway"`
		Leafnode        wireListenerTLS `json:"leaf"`
	} `json:"data"`
	Error *wireError `json:"error"`
}

type wireListenerTLS struct {
	TLSCertNotAfter time.Time `json:"tls_cert_not_after"`
}

type wireReloadResponse struct {
	Error *wireError `json:"error"`
}

type wireGatewayz struct {
	Outbound map[string]json.RawMessage   `json:"outbound_gateways"`
	Inbound  map[string][]json.RawMessage `json:"inbound_gateways"`
}

type wireGatewayzResponse struct {
	Server wireServerInfo `json:"server"`
	Data   *wireGatewayz  `json:"data"`
	Error  *wireError     `json:"error"`
}

func (w *wireGatewayz) gateways() *Gateways {
	g := &Gateways{Inbound: map[string]int{}}
	for name := range w.Outbound {
		g.Outbound = append(g.Outbound, name)
	}
	slices.Sort(g.Outbound)
	for name, conns := range w.Inbound {
		g.Inbound[name] = len(conns)
	}
	return g
}

type wirePeerRequest struct {
	Server string `json:"peer"`
}
