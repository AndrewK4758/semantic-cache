package chunker

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
)

var headerRegex = regexp.MustCompile(`^(#{1,6})\s+(.+)$`)

// Section represents a parsed markdown section.
type Section struct {
	Title string
	Level int32
	Lines []string
}

// SectionChunker chunks documents along section boundaries with table preservation and metadata injection.
type SectionChunker struct {
	maxTokens     int
	overlapTokens int
	tableChunker  *TableChunker
}

// NewSectionChunker initializes a SectionChunker with max tokens and overlap.
func NewSectionChunker(maxTokens, overlapTokens int) *SectionChunker {
	if maxTokens <= 0 {
		maxTokens = 512
	}
	if overlapTokens < 0 {
		overlapTokens = 50
	}
	return &SectionChunker{
		maxTokens:     maxTokens,
		overlapTokens: overlapTokens,
		tableChunker:  NewTableChunker(),
	}
}

func validateIdentity(identity *pb.InfrastructureIdentity) {
	if identity == nil {
		panic(fmt.Errorf("action failed: InfrastructureIdentity is required and cannot be nil"))
	}
	if strings.TrimSpace(identity.GetTenantId()) == "" {
		panic(fmt.Errorf("action failed: InfrastructureIdentity.TenantId is required"))
	}
	if strings.TrimSpace(identity.GetAppId()) == "" {
		panic(fmt.Errorf("action failed: InfrastructureIdentity.AppId is required"))
	}
	if strings.TrimSpace(identity.GetJobId()) == "" {
		panic(fmt.Errorf("action failed: InfrastructureIdentity.JobId is required"))
	}
}

// ParseSections splits raw Markdown into hierarchical sections.
func (sc *SectionChunker) ParseSections(doc string) []Section {
	lines := strings.Split(doc, "\n")
	var sections []Section
	var currentSection *Section

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		matches := headerRegex.FindStringSubmatch(trimmed)

		if len(matches) == 3 {
			if currentSection != nil && (len(currentSection.Lines) > 0 || currentSection.Title != "") {
				sections = append(sections, *currentSection)
			}
			currentSection = &Section{
				Level: int32(len(matches[1])),
				Title: strings.TrimSpace(matches[2]),
				Lines: nil,
			}
			continue
		}

		if currentSection == nil {
			currentSection = &Section{
				Level: 1,
				Title: "Overview",
				Lines: nil,
			}
		}

		currentSection.Lines = append(currentSection.Lines, line)
	}

	if currentSection != nil && (len(currentSection.Lines) > 0 || currentSection.Title != "") {
		sections = append(sections, *currentSection)
	}

	return sections
}

// ChunkDocument performs section-isolated chunking with parent metadata header injection.
func (sc *SectionChunker) ChunkDocument(doc, documentType string, identity *pb.InfrastructureIdentity) []*pb.SemanticChunk {
	validateIdentity(identity)

	if len(strings.TrimSpace(doc)) == 0 {
		return nil
	}

	sections := sc.ParseSections(doc)
	var allChunks []*pb.SemanticChunk
	chunkIndex := 0

	for _, sec := range sections {
		secBody := strings.TrimSpace(strings.Join(sec.Lines, "\n"))
		if len(secBody) == 0 && len(sec.Title) == 0 {
			continue
		}

		metaHeader := fmt.Sprintf("# Document: %s | Section: %s\n**Tenant:** %s | **App:** %s\n\n",
			documentType, sec.Title, identity.GetTenantId(), identity.GetAppId())
		metaHeaderTokens := EstimateTokens(metaHeader)
		effectiveMaxTokens := max(sc.maxTokens - metaHeaderTokens, 50)

		if sc.tableChunker.IsTable(secBody) {
			tableSegments := sc.tableChunker.ChunkTable(secBody, effectiveMaxTokens)
			for segIdx, tableSeg := range tableSegments {
				chunkIndex++
				fullContent := metaHeader + tableSeg
				allChunks = append(allChunks, &pb.SemanticChunk{
					ChunkId:              fmt.Sprintf("%s-%s-%d", identity.GetJobId(), sec.Title, chunkIndex),
					Content:              fullContent,
					SectionTitle:         sec.Title,
					SectionLevel:         sec.Level,
					ChunkIndex:           int32(chunkIndex),
					TokenCount:           int32(EstimateTokens(fullContent)),
					IsTableContinuation:  segIdx > 0,
				})
			}
			continue
		}

		secTokens := EstimateTokens(secBody)
		if secTokens <= effectiveMaxTokens {
			chunkIndex++
			fullContent := metaHeader + secBody
			allChunks = append(allChunks, &pb.SemanticChunk{
				ChunkId:      fmt.Sprintf("%s-%s-%d", identity.GetJobId(), sec.Title, chunkIndex),
				Content:      fullContent,
				SectionTitle: sec.Title,
				SectionLevel: sec.Level,
				ChunkIndex:   int32(chunkIndex),
				TokenCount:   int32(EstimateTokens(fullContent)),
			})
			continue
		}

		// Sub-chunk oversized section with sliding window overlap
		paragraphs := strings.Split(secBody, "\n\n")
		var currentBlock []string
		currentBlockTokens := 0

		for _, p := range paragraphs {
			pTrim := strings.TrimSpace(p)
			if len(pTrim) == 0 {
				continue
			}

			pTokens := EstimateTokens(pTrim)

			if len(currentBlock) > 0 && (currentBlockTokens+pTokens > effectiveMaxTokens) {
				chunkIndex++
				blockBody := strings.Join(currentBlock, "\n\n")
				fullContent := metaHeader + blockBody
				allChunks = append(allChunks, &pb.SemanticChunk{
					ChunkId:      fmt.Sprintf("%s-%s-%d", identity.GetJobId(), sec.Title, chunkIndex),
					Content:      fullContent,
					SectionTitle: sec.Title,
					SectionLevel: sec.Level,
					ChunkIndex:   int32(chunkIndex),
					TokenCount:   int32(EstimateTokens(fullContent)),
				})

				// Carry the trailing paragraph into the next chunk to preserve cross-boundary context.
				if sc.overlapTokens > 0 && len(currentBlock) > 1 {
					lastP := currentBlock[len(currentBlock)-1]
					currentBlock = []string{lastP, pTrim}
					currentBlockTokens = EstimateTokens(lastP) + pTokens
				} else {
					currentBlock = []string{pTrim}
					currentBlockTokens = pTokens
				}
				continue
			}

			currentBlock = append(currentBlock, pTrim)
			currentBlockTokens += pTokens
		}

		if len(currentBlock) > 0 {
			chunkIndex++
			blockBody := strings.Join(currentBlock, "\n\n")
			fullContent := metaHeader + blockBody
			allChunks = append(allChunks, &pb.SemanticChunk{
				ChunkId:      fmt.Sprintf("%s-%s-%d", identity.GetJobId(), sec.Title, chunkIndex),
				Content:      fullContent,
				SectionTitle: sec.Title,
				SectionLevel: sec.Level,
				ChunkIndex:   int32(chunkIndex),
				TokenCount:   int32(EstimateTokens(fullContent)),
			})
		}
	}

	return allChunks
}
