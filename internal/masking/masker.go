package masking

import (
	"sort"
	"strings"
	"sync"

	pb "github.com/AndrewK4758/shared_protos"
)

// EntitySurrogateRegistry maintains document-scoped entity mappings to natural surrogates.
type EntitySurrogateRegistry struct {
	mu           sync.RWMutex
	personCount  int
	locCount     int
	orgCount     int
	dateCount    int
	genericCount int
	rawToSurr    map[string]string
	surrToRaw    map[string]string
}

// NewEntitySurrogateRegistry creates a thread-safe registry.
func NewEntitySurrogateRegistry() *EntitySurrogateRegistry {
	return &EntitySurrogateRegistry{
		rawToSurr: make(map[string]string),
		surrToRaw: make(map[string]string),
	}
}

// indexToLetter converts 1 -> "A", 2 -> "B", ..., 26 -> "Z", 27 -> "AA".
func indexToLetter(n int) string {
	if n <= 0 {
		return "A"
	}
	result := ""
	for n > 0 {
		n--
		result = string(rune('A'+(n%26))) + result
		n /= 26
	}
	return result
}

// GetOrCreateSurrogate returns an existing natural surrogate or assigns a new sequential one.
func (r *EntitySurrogateRegistry) GetOrCreateSurrogate(entityType, rawValue string) string {
	normalizedKey := strings.TrimSpace(rawValue)
	if normalizedKey == "" {
		return ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if surrogate, exists := r.rawToSurr[normalizedKey]; exists {
		return surrogate
	}

	normalizedType := strings.ToUpper(strings.TrimSpace(entityType))
	var surrogate string

	switch normalizedType {
	case "PERSON", "BORROWER", "SELLER", "INDIVIDUAL", "NAME":
		r.personCount++
		surrogate = "Person " + indexToLetter(r.personCount)
	case "LOCATION", "ADDRESS", "PROPERTY", "STREET", "CITY", "STATE":
		r.locCount++
		surrogate = "Location " + string(rune('0'+r.locCount))
		if r.locCount >= 10 {
			surrogate = "Location " + strings.TrimSpace(string(rune('0'+(r.locCount/10)))) + string(rune('0'+(r.locCount%10)))
		}
	case "ORGANIZATION", "COMPANY", "LENDER", "BANK", "TITLE_COMPANY", "AGENCY":
		r.orgCount++
		surrogate = "Organization " + indexToLetter(r.orgCount)
	case "DATE", "CLOSING_DATE", "TIMESTAMP", "TIME":
		r.dateCount++
		surrogate = "Date " + string(rune('0'+r.dateCount))
	default:
		r.genericCount++
		surrogate = "Entity " + indexToLetter(r.genericCount)
	}

	r.rawToSurr[normalizedKey] = surrogate
	r.surrToRaw[surrogate] = normalizedKey
	return surrogate
}

// GetMap returns a copy of the raw-to-surrogate mapping.
func (r *EntitySurrogateRegistry) GetMap() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]string, len(r.rawToSurr))
	for k, v := range r.rawToSurr {
		result[k] = v
	}
	return result
}

// EntityMasker performs natural semantic token masking.
type EntityMasker struct {
	registry *EntitySurrogateRegistry
}

// NewEntityMasker initializes a new entity masker with a dedicated registry.
func NewEntityMasker() *EntityMasker {
	return &EntityMasker{
		registry: NewEntitySurrogateRegistry(),
	}
}

// MaskText applies natural surrogates to extracted entity values in the text.
func (m *EntityMasker) MaskText(text string, entities []*pb.ExtractedEntity) (string, map[string]string, int) {
	if len(text) == 0 || len(entities) == 0 {
		return text, m.registry.GetMap(), 0
	}

	// 1. First register surrogates in sequential input order
	for _, entity := range entities {
		rawVal := strings.TrimSpace(entity.GetRawValue())
		if len(rawVal) > 0 {
			m.registry.GetOrCreateSurrogate(entity.GetEntityType(), rawVal)
		}
	}

	// 2. Sort unique entries by raw_value length descending to prioritize longest string matches during replacement
	sortedEntities := make([]*pb.ExtractedEntity, len(entities))
	copy(sortedEntities, entities)
	sort.Slice(sortedEntities, func(i, j int) bool {
		return len(sortedEntities[i].GetRawValue()) > len(sortedEntities[j].GetRawValue())
	})

	totalReplacements := 0
	result := text

	for _, entity := range sortedEntities {
		rawVal := strings.TrimSpace(entity.GetRawValue())
		if len(rawVal) == 0 {
			continue
		}

		surrogate := m.registry.GetOrCreateSurrogate(entity.GetEntityType(), rawVal)
		count := strings.Count(result, rawVal)
		if count > 0 {
			result = strings.ReplaceAll(result, rawVal, surrogate)
			totalReplacements += count
		}
	}

	return result, m.registry.GetMap(), totalReplacements
}

