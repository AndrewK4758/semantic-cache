package masking_test

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/masking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntityMasker_SequentialNaturalSurrogates(t *testing.T) {
	masker := masking.NewEntityMasker()

	text := "Borrower John Doe and Co-Borrower Jane Smith purchased the property at 123 Main St, Atlanta, GA on 2026-08-15. John Doe signed the document."

	entities := []*pb.ExtractedEntity{
		{EntityType: "PERSON", RawValue: "John Doe", PageNumber: 1},
		{EntityType: "PERSON", RawValue: "Jane Smith", PageNumber: 1},
		{EntityType: "LOCATION", RawValue: "123 Main St, Atlanta, GA", PageNumber: 1},
		{EntityType: "DATE", RawValue: "2026-08-15", PageNumber: 1},
	}

	maskedText, surrogateMap, totalCount := masker.MaskText(text, entities)

	assert.Contains(t, maskedText, "Person A")
	assert.Contains(t, maskedText, "Person B")
	assert.Contains(t, maskedText, "Location 1")
	assert.Contains(t, maskedText, "Date 1")

	// Coreference: John Doe appears twice and both occurrences should map to Person A.
	assert.NotContains(t, maskedText, "John Doe")
	assert.NotContains(t, maskedText, "Jane Smith")
	assert.NotContains(t, maskedText, "123 Main St, Atlanta, GA")
	assert.NotContains(t, maskedText, "2026-08-15")

	assert.NotContains(t, maskedText, "[PERSON")
	assert.NotContains(t, maskedText, "<LOCATION")

	assert.Equal(t, "Person A", surrogateMap["John Doe"])
	assert.Equal(t, "Person B", surrogateMap["Jane Smith"])
	assert.Equal(t, "Location 1", surrogateMap["123 Main St, Atlanta, GA"])
	assert.Equal(t, "Date 1", surrogateMap["2026-08-15"])
	assert.Equal(t, 5, totalCount) // 2 John Doe + 1 Jane Smith + 1 Location + 1 Date
}

func TestEntityMasker_LongestEntityFirst(t *testing.T) {
	masker := masking.NewEntityMasker()

	text := "John Doe Jr. and John Doe were present."
	entities := []*pb.ExtractedEntity{
		{EntityType: "PERSON", RawValue: "John Doe", PageNumber: 1},
		{EntityType: "PERSON", RawValue: "John Doe Jr.", PageNumber: 1},
	}

	maskedText, surrogateMap, totalCount := masker.MaskText(text, entities)

	require.Equal(t, 2, totalCount)
	assert.Equal(t, "Person A", surrogateMap["John Doe"])
	assert.Equal(t, "Person B", surrogateMap["John Doe Jr."])
	assert.Equal(t, "Person B and Person A were present.", maskedText)

}

func TestEntityMasker_MultiPageConsistency(t *testing.T) {
	masker := masking.NewEntityMasker()

	page1 := "First American Title issued policy to John Doe."
	page2 := "John Doe agreed to settlement terms with First American Title."

	entities := []*pb.ExtractedEntity{
		{EntityType: "ORGANIZATION", RawValue: "First American Title", PageNumber: 1},
		{EntityType: "PERSON", RawValue: "John Doe", PageNumber: 1},
	}

	masked1, _, _ := masker.MaskText(page1, entities)
	masked2, surrogateMap, _ := masker.MaskText(page2, entities)

	assert.Equal(t, "Organization A", surrogateMap["First American Title"])
	assert.Equal(t, "Person A", surrogateMap["John Doe"])

	assert.Contains(t, masked1, "Organization A")
	assert.Contains(t, masked1, "Person A")
	assert.Contains(t, masked2, "Person A")
	assert.Contains(t, masked2, "Organization A")
}
