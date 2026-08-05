package domain

import "errors"

var (
	ErrInvalidIdentity = errors.New("invalid infrastructure identity: missing or blank tenant_id or app_id")
)
