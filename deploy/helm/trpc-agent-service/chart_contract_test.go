package chart_test

import (
	"os"
	"path/filepath"
	"sort"
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
	requireFragments(t, hpa, "apiVersion: autoscaling/v2", "kind: HorizontalPodAutoscaler", "averageUtilization:",
		"stabilizationWindowSeconds: 300", "type: External", "customMetrics", "averageValue:")
	requireFragments(t, values, "trpc_broker_backlog_total", "trpc_broker_delivery_lag_max_seconds",
		"trpc_audit_outbox_active_backlog", "trpc_audit_outbox_lag_max_seconds")
	requireFragments(t, network,
		"kind: NetworkPolicy",
		"default-deny",
		"policyTypes: [Ingress, Egress]",
		"protocol: UDP, port: 53",
		"$role.networkPolicy.egress",
	)
}

func TestMigrationHookContracts(t *testing.T) {
	job := read(t, filepath.Join("templates", "migration-job.yaml"))
	requireFragments(t, job,
		"kind: ServiceAccount",
		"kind: NetworkPolicy",
		"kind: Job",
		"helm.sh/hook: pre-install,pre-upgrade",
		"args: [\"schema-migrate\"]",
		"TRPC_MIGRATION_EXPECTED_CURRENT",
		"TRPC_MIGRATION_TARGET",
		"restartPolicy: Never",
		"automountServiceAccountToken:",
		"enableServiceLinks: false",
	)
	if strings.Contains(job, "TRPC_POSTGRES_DSN\n              value:") {
		t.Fatal("migration DSN must come from an existing role-scoped Secret")
	}
}

func TestMigrationTargetTracksLatestEmbeddedMigration(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("embedded migrations: files=%v err=%v", files, err)
	}
	sort.Strings(files)
	latest := strings.SplitN(filepath.Base(files[len(files)-1]), "_", 2)[0]
	values := read(t, "values.yaml")
	if !strings.Contains(values, "target: \""+latest+"\"") {
		t.Fatalf("chart migration target does not track latest embedded migration %s", latest)
	}
}

func TestProductionMainSubscribesToTerminationSignals(t *testing.T) {
	mainSource := read(t, filepath.Join("..", "..", "..", "cmd", "trpc-service", "main.go"))
	requireFragments(t, mainSource,
		"signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)",
		"case \"prestop\":",
		"runPreStop(getenv, syscall.Kill, time.Sleep)",
	)
}
