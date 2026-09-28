package natscluster

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
)

// ReasonGatewayWithoutTLS is the Ready and Progressing reason of a
// NatsCluster refused for a gateway without tls.
const ReasonGatewayWithoutTLS = "GatewayWithoutTLS"

// FlagAllowGatewayWithoutTLS is the cluster controller's flag that sets
// Reconciler.AllowGatewayWithoutTLS.
const FlagAllowGatewayWithoutTLS = "allow-gateway-without-tls"

const gatewayWithoutTLSMessage = "gateway.tls is unset, and a gateway has no authentication without it; set it, or run the cluster controller with --" + FlagAllowGatewayWithoutTLS

// gatewayWithoutTLS reports whether spec renders a gateway in the clear
// while allow is false.
func gatewayWithoutTLS(spec *clusterv1beta1.NatsClusterSpec, allow bool) bool {
	return !allow && spec.Gateway != nil && spec.Gateway.TLS == nil
}

// refuseGatewayWithoutTLS sets nc's Ready and Progressing conditions False
// with reason GatewayWithoutTLS and patches the status.
func (r *Reconciler) refuseGatewayWithoutTLS(ctx context.Context, orig, nc *clusterv1beta1.NatsCluster) error {
	conditions.Set(&nc.Status.Conditions, nc.Generation, metav1.Condition{
		Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonGatewayWithoutTLS, Message: gatewayWithoutTLSMessage})
	return r.hold(ctx, orig, nc, &metav1.Condition{
		Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonGatewayWithoutTLS, Message: gatewayWithoutTLSMessage})
}
