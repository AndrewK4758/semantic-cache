package chunker_test

import (
	"strings"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/chunker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableChunker_RowAtomicSplittingAndHeaderRepetition(t *testing.T) {
	tc := chunker.NewTableChunker()

	tableMarkdown := `| Fee Description | Paid By Borrower | Paid By Seller |
|---|---|---|
| Loan Origination Fee | $1,200.00 | $0.00 |
| Appraisal Fee | $550.00 | $0.00 |
| Credit Report Fee | $45.00 | $0.00 |
| Title Insurance | $1,850.00 | $0.00 |
| Settlement Agent Fee | $750.00 | $0.00 |
| Transfer Tax | $0.00 | $2,100.00 |
| Recording Fees | $125.00 | $0.00 |`

	// Max token limit small enough to force a split (e.g. ~40 tokens / 150 chars)
	chunks := tc.ChunkTable(tableMarkdown, 35)

	require.GreaterOrEqual(t, len(chunks), 2, "Should split table into at least 2 chunks")

	for i, chunk := range chunks {
		lines := strings.Split(strings.TrimSpace(chunk), "\n")
		require.GreaterOrEqual(t, len(lines), 3, "Each chunk must contain header, delimiter, and at least one data row")
		assert.Equal(t, "| Fee Description | Paid By Borrower | Paid By Seller |", lines[0], "Chunk %d must start with column header", i)
		assert.Equal(t, "|---|---|---|", lines[1], "Chunk %d must have delimiter on second line", i)

		for _, line := range lines {
			assert.True(t, strings.HasPrefix(line, "|") && strings.HasSuffix(line, "|"), "Table rows must be intact: %s", line)
		}
	}
}

func TestSectionChunker_SectionIsolationAndOverlap(t *testing.T) {
	sc := chunker.NewSectionChunker(100, 20)

	doc := `# Document Details
This is the overview of the loan closing packet. Everything is verified.

## Loan Terms
Loan amount is $450,000.00 with interest rate at 6.25% fixed for 30 years. The monthly principal and interest payment is $2,770.00. Borrower must maintain escrow.

## Closing Costs
| Item | Amount |
|---|---|
| Closing Fee | $500.00 |
| Legal Fee | $800.00 |`

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant-100",
		AppId:    "app-mployee",
		JobId:    "job-999",
	}

	chunks := sc.ChunkDocument(doc, "ClosingDisclosure", identity)

	require.NotEmpty(t, chunks)

	for _, chunk := range chunks {
		assert.Contains(t, chunk.GetContent(), "# Document: ClosingDisclosure | Section:")
		assert.Contains(t, chunk.GetContent(), "**Tenant:** tenant-100 | **App:** app-mployee")
		assert.NotEmpty(t, chunk.GetSectionTitle())
		assert.Greater(t, chunk.GetTokenCount(), int32(0))
	}

	// Verify section isolation: Closing Costs chunk must NOT contain Loan Terms body
	var closingCostsChunk *pb.SemanticChunk
	for _, chunk := range chunks {
		if chunk.GetSectionTitle() == "Closing Costs" {
			closingCostsChunk = chunk
			break
		}
	}
	require.NotNil(t, closingCostsChunk, "Must have a chunk for Closing Costs")
	assert.NotContains(t, closingCostsChunk.GetContent(), "monthly principal and interest")
}

func TestChunkAssembler_FailFastOnMissingIdentity(t *testing.T) {
	sc := chunker.NewSectionChunker(512, 50)

	assert.Panics(t, func() {
		sc.ChunkDocument("Sample text", "DocType", nil)
	}, "Must panic when InfrastructureIdentity is nil")

	assert.Panics(t, func() {
		sc.ChunkDocument("Sample text", "DocType", &pb.InfrastructureIdentity{TenantId: "", AppId: "app", JobId: "job"})
	}, "Must panic when TenantId is empty")
}
