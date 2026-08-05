package messaging

import "github.com/AndrewK4758/shared_utils/payloadkeys"

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/workflowparser"
	"google.golang.org/protobuf/types/known/structpb"
)

func createTestStruct(m map[string]interface{}) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

// TestM2Challenger_SemanticCache_NodeNameStrict verifies strict node_name extraction
func TestM2Challenger_SemanticCache_NodeNameStrict(t *testing.T) {
	// Scenario 1: Valid node_name in node_config
	validPayload := createTestStruct(map[string]interface{}{
		payloadkeys.NodeConfigKey: map[string]interface{}{
			"node_name": "SemanticCacheWorker",
		},
	})
	reqValid := &pb.ExecuteTaskRequest{Payload: validPayload}
	parserValid := workflowparser.NewTaskRequestParser(reqValid)
	nodeName, err := parserValid.GetNodeNameStrict()
	if err != nil || nodeName != "SemanticCacheWorker" {
		t.Fatalf("expected node_name 'SemanticCacheWorker', got %q, err %v", nodeName, err)
	}

	// Scenario 2: Legacy PascalCase NodeName at node_config -> Must fail fast
	legacyPayload := createTestStruct(map[string]interface{}{
		payloadkeys.NodeConfigKey: map[string]interface{}{
			"NodeName": "SemanticCacheWorker",
		},
	})
	reqLegacy := &pb.ExecuteTaskRequest{Payload: legacyPayload}
	parserLegacy := workflowparser.NewTaskRequestParser(reqLegacy)
	_, errLegacy := parserLegacy.GetNodeNameStrict()
	if errLegacy == nil {
		t.Fatalf("CRITICAL SECURITY / DRIFT FAIL: GetNodeNameStrict accepted legacy PascalCase 'NodeName'")
	}

	// Scenario 3: Root payload node_name -> Must fail fast
	rootPayload := createTestStruct(map[string]interface{}{
		"node_name": "SemanticCacheWorker",
	})
	reqRoot := &pb.ExecuteTaskRequest{Payload: rootPayload}
	parserRoot := workflowparser.NewTaskRequestParser(reqRoot)
	_, errRoot := parserRoot.GetNodeNameStrict()
	if errRoot == nil {
		t.Fatalf("CRITICAL SECURITY / DRIFT FAIL: GetNodeNameStrict read legacy root payload 'node_name'")
	}
}
