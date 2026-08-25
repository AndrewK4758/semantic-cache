package sparse

import (
	"math"
	"slices"
	"strings"
	"unicode"
)

const maxSparseIndex = 1048576 // 2^20

// Vector SparseVector holds sorted indices and their non-zero weights.
type Vector struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// Generator SparseGenerator converts text into term-frequency sparse vectors.
type Generator struct{}

// NewSparseGenerator creates a new sparse vector generator.
func NewSparseGenerator() *Generator {
	return &Generator{}
}

func fnv1aHash(s string) uint32 {
	var hash uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= 16777619
	}
	return hash % maxSparseIndex
}

func tokenize(text string) []string {
	var b strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteByte(' ')
		}
	}
	return strings.Fields(b.String())
}

// Generate builds a deterministic sparse vector from input text.
func (g *Generator) Generate(text string) Vector {
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return Vector{
			Indices: nil,
			Values:  nil,
		}
	}

	// Compute term frequencies per token
	tfMap := make(map[uint32]float32)
	for _, tok := range tokens {
		if len(tok) == 0 {
			continue
		}
		idx := fnv1aHash(tok)
		tfMap[idx] += 1.0
	}

	if len(tfMap) == 0 {
		return Vector{}
	}

	// Collect unique indices and sort them in ascending order
	indices := make([]uint32, 0, len(tfMap))
	for idx := range tfMap {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	// Compute L2 norm for TF values (log-scaled: 1 + ln(tf))
	var normSq float64
	scaledValues := make([]float32, len(indices))
	for i, idx := range indices {
		tf := tfMap[idx]
		val := float32(1.0 + math.Log(float64(tf)))
		scaledValues[i] = val
		normSq += float64(val * val)
	}

	norm := float32(math.Sqrt(normSq))
	if norm > 0 {
		for i := range scaledValues {
			scaledValues[i] /= norm
		}
	}

	return Vector{
		Indices: indices,
		Values:  scaledValues,
	}
}
