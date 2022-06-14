package importer

import (
	"context"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	creds "github.com/leapforce-libraries/go_creds"
	errortools "github.com/leapforce-libraries/go_errortools"
)

// type table is an interface that contains all necessary functionality to synchronize an api-call with a BigQuery table.
//
type Importable interface {
	Table() *Table
	GetDataAndWriteToBucket(ctx context.Context, obj *storage.ObjectHandle, softwareClientLicense *creds.SoftwareClientLicense, startDate *civil.Date, endDate *civil.Date) (int, *errortools.Error)
}
