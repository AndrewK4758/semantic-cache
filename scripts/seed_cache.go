package main

import (
	"context"
	"log"
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
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSemanticCacheServiceClient(conn)

	items := []SeedItem{
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"classification": "Title_Opinion"}`,
			SubjectClass: "Attorney Service Ordered",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"classification": "Closing"}`,
			SubjectClass: "Closing Service Ordered",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"classification": "Deed"}`,
			SubjectClass: "Deed Service Updated",
		},
		{
			Collection:   "incoming_email_templates",
			JsonPayload:  `{"classification": "Deed"}`,
			SubjectClass: "Deed Service Ordered",
		},
	}

	for _, item := range items {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		req := &pb.SeedCacheRequest{
			CollectionName:   item.Collection,
			TemplateText:     item.SubjectClass,
			Identity:         &pb.InfrastructureIdentity{JobId: "seed"},
			ExtractedPayload: item.JsonPayload,
		}

		res, err := client.SeedCache(ctx, req)
		if err != nil {
			log.Printf("Failed to seed %s: %v", item.SubjectClass, err)
		} else {
			log.Printf("Seeded %s: %s", item.SubjectClass, res.Message)
		}
		cancel()
	}
}
