package chart_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func read(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func requireFragments(t *testing.T, content string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(content, fragment) {
			t.Errorf("missing chart contract fragment %q", fragment)
		}
	}
}

func TestWorkloadProductionContracts(t *testing.T) {
	workloads := read(t, filepath.Join("templates", "workloads.yaml"))
	requireFragments(t, workloads,
		"serviceAccountName:",
		"automountServiceAccountToken:",
		"enableServiceLinks: false",
		"terminationGracePeriodSeconds:",
		"readinessProbe:",
		"path: /readyz",
		"livenessProbe:",
		"path: /livez",
		"startupProbe:",
		"command: [\"/trpc-service\", \"prestop\"]",
		"TRPC_PRESTOP_TARGET_PID",
		"topologySpreadConstraints:",
		"podAntiAffinity:",
		"driver: secrets-store.csi.k8s.io",
		"secretProviderClass is required when secretProvider.enabled=true",
	)
	if strings.Contains(workloads, "kind: Secret") || strings.Contains(workloads, "kind: ConfigMap") {
		t.Fatal("workload chart must not synthesize secret-bearing resources")
	}
}

func TestAvailabilityAndNetworkContracts(t *testing.T) {
	values := read(t, "values.yaml")
	requireFragments(t, values,
		"runAsNonRoot: true",
		"readOnlyRootFilesystem: true",
		"drop: [\"ALL\"]",
	)
	for _, role := range []string{"gateway:", "worker:", "channel:", "channel-delivery:", "preprocess:", "artifact:", "audit-relay:"} {
		requireFragments(t, values, role)
	}
	requireFragments(t, values,
		"worker:\n    enabled: true",
		"terminationGracePeriodSeconds: 75",
		"minReplicas: 3",
		"maxReplicas: 30",
	)

	pdb := read(t, filepath.Join("templates", "pdbs.yaml"))
	hpa := read(t, filepath.Join("templates", "hpas.yaml"))
	network := read(t, filepath.Join("templates", "networkpolicies.yaml"))
	requireFragments(t, pdb, "apiVersion: policy/v1", "kind: PodDisruptionBudget", "minAvailable:")
	requireFragments(t, hpa, "apiVersion: autoscaling/v2", "kind: HorizontalPodAutoscaler", "averageUtilization:")
	requireFragments(t, network,
		"kind: NetworkPolicy",
		"default-deny",
		"policyTypes: [Ingress, Egress]",
		"protocol: UDP, port: 53",
		"$role.networkPolicy.egress",
	)
}

func TestProductionMainSubscribesToTerminationSignals(t *testing.T) {
	mainSource := read(t, filepath.Join("..", "..", "..", "cmd", "trpc-service", "main.go"))
	requireFragments(t, mainSource,
		"signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)",
		"case \"prestop\":",
		"runPreStop(getenv, syscall.Kill, time.Sleep)",
	)
}
