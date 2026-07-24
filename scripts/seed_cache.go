package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/AndrewK4758/shared_protos"
)

type SeedItem struct {
	Collection   string `json:"collection"`
	JsonPayload  string `json:"json_payload"`
	SubjectClass string `json:"subject_class"`
}

func main() {
	conn, err := grpc.Dial("localhost:50055", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewSemanticCacheServiceClient(conn)

	items := []SeedItem{
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"DocumentType": "TITLE_OPINION"}`,
			SubjectClass: "Attorney Service Ordered",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"DocumentType": "CLOSING"}`,
			SubjectClass: "Closing Service Ordered",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"DocumentType": "CLOSING"}`,
			SubjectClass: "Deed Service Updated",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"DocumentType": "CLOSING"}`,
			SubjectClass: "Deed Service Ordered",
		},
	}

	for _, item := range items {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		req := &pb.SeedCacheRequest{
			CollectionName:   item.Collection,
			TemplateText:     item.SubjectClass,
			Identity:         &pb.InfrastructureIdentity{JobId: "seed", TenantId: "CNJ", AppId: "Artemis"},
			ExtractedPayload: item.JsonPayload,
			Metadata:         map[string]string{"subject_class": item.SubjectClass},
		}

		res, err := client.SeedCache(ctx, req)
		if err != nil {
			fmt.Printf("Failed to seed %s: %v\n", item.SubjectClass, err)
		} else {
			fmt.Printf("Seeded %s: %s\n", item.SubjectClass, res.Message)
		}
		cancel()
	}
}
