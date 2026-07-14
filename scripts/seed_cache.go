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
	SubjectClass   string
	Classification string
	Template       string
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
			SubjectClass:   "Attorney Service Ordered",
			Classification: "Title_Opinion",
			Template: `Subject:

Due Date & Time:
Attorney Service Ordered by Rocket Close
Attorney Opinion - Refinance
Service Information
Attorney Opinion - Refinance:
Instructions:
Order Information
Order #:
Order Date:
Order Type:
Transaction Type:
Purchase Price:
Primary Loan #:
Primary Loan Type:
Primary Loan Description:
Primary Loan Amount:
Primary Proposed Insured Lender:
Property Address:
Property County:
Property Tax Ids:
Brief Legal:
Borrower - 
Mobile:
Middle Initial:
Is Married:
DOB:
SSN:
Borrower - 
Mobile:
Middle Initial:
Is Married:
DOB:
SSN:
Return Completed Orders using the Rocket Close Website or send to:
Rocket Close, LLC
662 Woodward Avenue, Detroit, MI 48226
Email:
Fax:
Phone:`,
		},
		{
			SubjectClass:   "Closing Service Ordered",
			Classification: "Closing",
			Template: `Subject:

Closing Services Ordered by Rocket Close
Closing Information
Scheduled Closing Date and Time:
Closing Location:
Language:
Vendor Information
Number:
Name:
Address:
Phone:
Fax:
Email:
Service Information
Attorney Hybrid Refinance Signing:
Hybrid Online Documents:
Client:
Closing Loan #:
Instructions:
Order Information
Order #:
Order Date:
Order Type:
Transaction Type:
Purchase Price:
Primary Loan #:
Primary Loan Type:
Primary Loan Description:
Primary Loan Amount:
Primary Proposed Insured Lender:
Property Address:
Property County:
Borrower - 
Mobile:
Borrower - 
Mobile:`,
		},
		{
			SubjectClass:   "Deed Service Updated",
			Classification: "Deed",
			Template: `Subject:
 
Deed Service Updated by Rocket Close
Deed - Correction Affidavit
Service Information
Deed - Correction Affidavit:
Property Address:
Property County:
Vesting Type:
Grantor / Seller:
Grantee / Buyer:
Instructions:
Order Information
Order #:
Order Date:
Order Type:
Transaction Type:
Purchase Price:
Primary Loan #:
Primary Loan Type:
Primary Loan Description:
Primary Loan Amount:
Primary Proposed Insured Lender:
Property Address:
Property County:
Borrower - 
Home:
Work:
Middle Initial:
Is Married:
DOB:
SSN:`,
		},
		{
			SubjectClass:   "Deed Service Ordered",
			Classification: "Deed",
			Template: `Subject:
 
Deed Service Ordered by Rocket Close
Deed - Warranty Deed-Standard-Purchase
Service Information
Deed - Warranty Deed-Standard-Purchase:
Property Address:
Property County:
Vesting Type:
Grantor / Seller:
Grantee / Buyer:
Instructions:
Order Information
Order #:
Order Date:
Order Type:
Transaction Type:
Purchase Price:
Primary Loan #:
Primary Loan Type:
Primary Loan Description:
Primary Loan Amount:
Primary Proposed Insured Lender:
Property Address:
Property County:
Brief Legal:
Borrower - 
Home:
Mobile:
Middle Initial:
Is Married:
SSN:`,
		},
	}

	for _, item := range items {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		req := &pb.SeedCacheRequest{
			TemplateText: item.Template,
			Metadata: map[string]string{
				"collection":    "incoming_email_templates",
				"subject_class": item.SubjectClass,
			},
			ExtractedPayload: `{"classification": "` + item.Classification + `"}`,
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
