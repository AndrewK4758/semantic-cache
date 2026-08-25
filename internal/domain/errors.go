package domain

import "errors"

var (
	ErrRecordNotFound     = errors.New("record not found")
	ErrCollectionRequired = errors.New("collection name is required")
)

