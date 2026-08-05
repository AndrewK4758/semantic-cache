package application_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
)

// TestEmpirical_JobIDIndependence verifies that different Job IDs with identical tenant, app, and cleaned text
// produce EXACTLY the same SHA256 record ID.
func TestEmpirical_JobIDIndependence(t *testing.T) {
	jobIDs := []string{
		"job-1001",
		"job-9999",
		"550e8400-e29b-41d4-a716-446655440000",
		"",
		"random-job-id-xyz",
	}

	tenantID := "tenant-acme-corp"
	appID := "app-document-processor"
	text := "Cleaned document text representation for semantic caching."

	var baselineID string

	for i, jobID := range jobIDs {
		identity := &pb.InfrastructureIdentity{
			TenantId: tenantID,
			AppId:    appID,
			JobId:    jobID,
		}

		recordID, err := application.GenerateRecordID(text, identity)
		if err != nil {
			t.Fatalf("unexpected error for jobID %q: %v", jobID, err)
		}

		if i == 0 {
			baselineID = recordID
		} else {
			if recordID != baselineID {
				t.Errorf("Job ID mismatch! Baseline (jobID=%q): %s, Current (jobID=%q): %s",
					jobIDs[0], baselineID, jobID, recordID)
			}
		}
	}

	// Verify exact SHA256 manual computation match
	expectedInput := fmt.Sprintf("%d:%s|%d:%s|%d:%s", len(tenantID), tenantID, len(appID), appID, len(text), text)
	expectedHash := sha256.Sum256([]byte(expectedInput))
	expectedHex := hex.EncodeToString(expectedHash[:])

	if baselineID != expectedHex {
		t.Errorf("GenerateRecordID produced %s, but expected manual hash %s", baselineID, expectedHex)
	}
}

// TestEmpirical_BoundaryShiftResilience verifies that string boundary shifts produce DIFFERENT SHA256 record IDs
// due to length prefixing framing.
func TestEmpirical_BoundaryShiftResilience(t *testing.T) {
	testCases := []struct {
		name     string
		tenantID string
		appID    string
		text     string
	}{
		// Case 1: Simple boundary shift between tenantID and appID
		{"Shift T->A 1", "a", "bc", "xyz"},
		{"Shift T->A 2", "ab", "c", "xyz"},
		{"Shift T->A 3", "abc", "", "xyz"}, // Note: empty appId should be rejected by validation if passed to GenerateRecordID

		// Case 2: Delimiter '|' injection across boundaries
		{"Pipe Inj 1", "a|b", "c", "d"},
		{"Pipe Inj 2", "a", "b|c", "d"},
		{"Pipe Inj 3", "a", "b", "c|d"},

		// Case 3: Colon ':' and '|' combination injection
		{"Colon Inj 1", "1:a", "b", "c"},
		{"Colon Inj 2", "1", "a:b", "c"},
		{"Colon Inj 3", "1", "a", ":bc"},
		{"Colon-Pipe Inj", "1:a|b", "c", "d"},

		// Case 4: Unicode boundary shifts (multi-byte UTF-8 chars)
		{"Unicode 1", "🚀", "bar", "text"},
		{"Unicode 2", "🚀b", "ar", "text"},
	}

	seenHashes := make(map[string]string) // hash -> test case name

	for _, tc := range testCases {
		if tc.tenantID == "" || tc.appID == "" {
			// Skip cases with empty tenant/app as validation will reject them
			continue
		}

		identity := &pb.InfrastructureIdentity{
			TenantId: tc.tenantID,
			AppId:    tc.appID,
			JobId:    "job-const",
		}

		hash, err := application.GenerateRecordID(tc.text, identity)
		if err != nil {
			t.Fatalf("unexpected error for case %s: %v", tc.name, err)
		}

		if prevCase, found := seenHashes[hash]; found {
			t.Errorf("HASH COLLISION DETECTED! Case %q and Case %q both generated SHA256: %s",
				tc.name, prevCase, hash)
		}
		seenHashes[hash] = tc.name
	}
}

// TestEmpirical_GenerateRecordID_ValidationEdgeCases verifies robust error handling for invalid identities.
func TestEmpirical_GenerateRecordID_ValidationEdgeCases(t *testing.T) {
	invalidCases := []struct {
		name     string
		identity *pb.InfrastructureIdentity
		text     string
	}{
		{"Nil identity", nil, "text"},
		{"Empty tenant", &pb.InfrastructureIdentity{TenantId: "", AppId: "app", JobId: "job"}, "text"},
		{"Whitespace tenant", &pb.InfrastructureIdentity{TenantId: "   ", AppId: "app", JobId: "job"}, "text"},
		{"Empty app", &pb.InfrastructureIdentity{TenantId: "tenant", AppId: "", JobId: "job"}, "text"},
		{"Whitespace app", &pb.InfrastructureIdentity{TenantId: "tenant", AppId: "\t", JobId: "job"}, "text"},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := application.GenerateRecordID(tc.text, tc.identity)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

// TestEmpirical_GenerateRecordID_EmptyText verifies hashing with empty text string.
func TestEmpirical_GenerateRecordID_EmptyText(t *testing.T) {
	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_t",
		AppId:    "app_a",
		JobId:    "job_j",
	}

	hash, err := application.GenerateRecordID("", identity)
	if err != nil {
		t.Fatalf("unexpected error for empty text: %v", err)
	}

	expectedInput := fmt.Sprintf("%d:%s|%d:%s|%d:%s", len("tenant_t"), "tenant_t", len("app_a"), "app_a", 0, "")
	expectedHash := sha256.Sum256([]byte(expectedInput))
	expectedHex := hex.EncodeToString(expectedHash[:])

	if hash != expectedHex {
		t.Errorf("expected %s, got %s", expectedHex, hash)
	}
}
