package qdrant

import (
	"testing"

	docpb "github.com/AndrewK4758/shared_protos"
)

func TestBuildConditions(t *testing.T) {
	t.Run("Nil identity and nil metadata", func(t *testing.T) {
		conds := buildConditions(nil, nil)
		if len(conds) != 0 {
			t.Fatalf("expected 0 conditions, got %d", len(conds))
		}
	})

	t.Run("Full identity and metadata", func(t *testing.T) {
		identity := &docpb.InfrastructureIdentity{
			TenantId: "tenant-1",
			AppId:    "app-1",
			JobId:    "job-1",
		}
		metadata := map[string]any{
			"key1": "val1",
			"key2": 123, // non-string should be skipped gracefully
		}
		conds := buildConditions(identity, metadata)
		// Expected: tenant_id, app_id, job_id, key1 = 4 conditions
		if len(conds) != 4 {
			t.Fatalf("expected 4 conditions, got %d", len(conds))
		}
	})

	t.Run("Partial identity with empty fields", func(t *testing.T) {
		identity := &docpb.InfrastructureIdentity{
			TenantId: "tenant-1",
			AppId:    "",
			JobId:    "",
		}
		conds := buildConditions(identity, nil)
		if len(conds) != 1 {
			t.Fatalf("expected 1 condition for tenant_id, got %d", len(conds))
		}
	})
}
