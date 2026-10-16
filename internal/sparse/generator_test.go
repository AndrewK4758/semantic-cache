package sparse_test

import (
	"testing"

	"github.com/doc_processor/semantic_cache_service/internal/sparse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparseGenerator_DeterministicAndSorted(t *testing.T) {
	gen := sparse.NewSparseGenerator()

	text := "Loan closing disclosure for borrower John Doe on property 123 Main St. Borrower agreed."

	vec1 := gen.Generate(text)
	vec2 := gen.Generate(text)

	require.NotEmpty(t, vec1.Indices)
	require.Equal(t, len(vec1.Indices), len(vec1.Values))

	assert.Equal(t, vec1.Indices, vec2.Indices)
	assert.Equal(t, vec1.Values, vec2.Values)

	for i := 1; i < len(vec1.Indices); i++ {
		assert.Greater(t, vec1.Indices[i], vec1.Indices[i-1], "Indices must be strictly increasing")
	}

	for _, val := range vec1.Values {
		assert.Greater(t, val, float32(0.0))
	}
}

func TestSparseGenerator_EmptyText(t *testing.T) {
	gen := sparse.NewSparseGenerator()

	vec := gen.Generate("   ")
	assert.Empty(t, vec.Indices)
	assert.Empty(t, vec.Values)
}

func TestSparseGenerator_PunctuationAndCaseInsensitivity(t *testing.T) {
	gen := sparse.NewSparseGenerator()

	vec1 := gen.Generate("Settlement Charges: Loan Disclosure!")
	vec2 := gen.Generate("settlement charges loan disclosure")

	assert.Equal(t, vec1.Indices, vec2.Indices)
	assert.Equal(t, vec1.Values, vec2.Values)
}

