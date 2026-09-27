// Package conditions sets status conditions stamped with the generation
// they were judged at.
package conditions

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Set sets c on conds with ObservedGeneration gen, and reports whether
// conds changed. LastTransitionTime moves only when c's status does.
func Set(conds *[]metav1.Condition, gen int64, c metav1.Condition) bool {
	c.ObservedGeneration = gen
	return meta.SetStatusCondition(conds, c)
}

// Status is True when on and False otherwise.
func Status(on bool) metav1.ConditionStatus {
	if on {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
