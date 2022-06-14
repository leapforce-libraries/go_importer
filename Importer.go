package importer

import (
	"context"
	"fmt"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	creds "github.com/leapforce-libraries/go_creds"
	errortools "github.com/leapforce-libraries/go_errortools"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	ig "github.com/leapforce-libraries/go_integration"
	go_types "github.com/leapforce-libraries/go_types"
)

type Importer struct {
	context               context.Context
	config                *ig.Config
	bigQueryService       *go_bigquery.Service
	bucketHandle          *storage.BucketHandle
	softwareClientLicense *creds.SoftwareClientLicense
}

type ImporterConfig struct {
	Context               *context.Context
	Config                *ig.Config
	BigQueryService       *go_bigquery.Service
	BucketHandle          *storage.BucketHandle
	SoftwareClientLicense *creds.SoftwareClientLicense
}

func NewImporter(cfg *ImporterConfig) (*Importer, *errortools.Error) {
	if cfg == nil {
		return nil, errortools.ErrorMessage("ImporterConfig is nil pointer")
	}
	if cfg.Config == nil {
		return nil, errortools.ErrorMessage("Config is nil pointer")
	}
	if cfg.BigQueryService == nil {
		return nil, errortools.ErrorMessage("BigQueryService is nil pointer")
	}
	if cfg.BucketHandle == nil {
		return nil, errortools.ErrorMessage("BucketHandle is nil pointer")
	}

	importer := Importer{
		config:                cfg.Config,
		bigQueryService:       cfg.BigQueryService,
		bucketHandle:          cfg.BucketHandle,
		softwareClientLicense: cfg.SoftwareClientLicense,
	}

	if cfg.Context != nil {
		importer.context = context.TODO()
	}

	return &importer, nil
}

func (importer *Importer) Config() *ig.Config {
	return importer.config
}

func (importer *Importer) BigQueryService() *go_bigquery.Service {
	return importer.bigQueryService
}

func (importer *Importer) ProcessTable(importable Importable, startDate *civil.Date, endDate *civil.Date) *errortools.Error {
	if startDate == nil {
		startDate = ig.TomorrowPtr()
	}

	if endDate == nil {
		endDate = ig.TomorrowPtr()
	}

	_bigQueryDataset := importer.config.Dataset
	_objectName := importable.Table().Name
	ig.WithEnvironment(&_bigQueryDataset, &_objectName)

	sqlConfigTarget := go_bigquery.SqlConfig{
		DatasetName:     _bigQueryDataset,
		TableOrViewName: &importable.Table().Name,
		ModelOrSchema:   importable.Table().Schema,
	}

	tableTargetExists, e := importer.bigQueryService.TableExists(&sqlConfigTarget)
	if e != nil {
		return e
	}

	guid := go_types.NewGuid()
	tempTableName := importable.Table().Name + "_" + (&guid).String()

	sqlConfigTemp := go_bigquery.SqlConfig{
		DatasetName:     _bigQueryDataset,
		TableOrViewName: &tempTableName,
		ModelOrSchema:   importable.Table().Schema,
	}

	objectHandles := []*storage.ObjectHandle{}

	// create object handle
	guid = go_types.NewGuid()
	obj := importer.bucketHandle.Object(fmt.Sprintf("%s_%s", _objectName, guid.String()))
	oa := storage.ObjectAttrsToUpdate{}
	oa.ContentType = "application/json"
	obj.Update(importer.context, oa)

	// get data and write to object
	rowCount, e := importable.GetDataAndWriteToBucket(importer.context, obj, importer.softwareClientLicense, startDate, endDate)
	if e != nil {
		return e
	}

	if importable.Table().Merge != nil {
		// MERGE
		if rowCount > 0 {
			copyObjectToTableConfig := go_bigquery.CopyObjectToTableConfig{
				ObjectHandle:  obj,
				SqlConfig:     &sqlConfigTemp,
				TruncateTable: false,
				DeleteObject:  true,
			}

			if !tableTargetExists {
				copyObjectToTableConfig.SqlConfig = &sqlConfigTarget
			}

			e = importer.bigQueryService.CopyObjectToTable(&copyObjectToTableConfig)
			if e != nil {
				return e
			}

			if tableTargetExists {
				// merge data
				joinFields := importable.Table().Merge.JoinFields
				joinFields = append(joinFields, "SoftwareClientLicenseGuid_")

				e := importer.bigQueryService.Merge(&sqlConfigTemp, &sqlConfigTarget, joinFields, &importable.Table().Merge.DoNotUpdateFields, false)
				if e != nil {
					return e
				}

				// delete temp table
				e = importer.bigQueryService.DeleteTable(&sqlConfigTemp)
				if e != nil {
					return e
				}
			}
		}
	} else if importable.Table().Append != nil || importable.Table().Replace != nil {
		// APPEND OR REPLACE
		if rowCount > 0 {
			objectHandles = append(objectHandles, obj)
		}

		if importable.Table().Replace != nil {
			if tableTargetExists {
				// delete existing data
				e = importer.deleteData(importer.softwareClientLicense, importable, importer.config, importer.bigQueryService, startDate, endDate)
				if e != nil {
					return e
				}
			}
		}

		for _, obj := range objectHandles {
			// copy data to BigQuery table
			copyObjectToTableConfig := go_bigquery.CopyObjectToTableConfig{
				ObjectHandle:  obj,
				SqlConfig:     &sqlConfigTarget,
				TruncateTable: false,
				DeleteObject:  true,
			}

			e := importer.bigQueryService.CopyObjectToTable(&copyObjectToTableConfig)
			if e != nil {
				return e
			}
		}
	} else if importable.Table().Truncate != nil {
		// TRUNCATE
		for i, obj := range objectHandles {
			// copy data to BigQuery table
			copyObjectToTableConfig := go_bigquery.CopyObjectToTableConfig{
				ObjectHandle:  obj,
				SqlConfig:     &sqlConfigTarget,
				TruncateTable: i == 0,
				DeleteObject:  true,
			}

			e := importer.bigQueryService.CopyObjectToTable(&copyObjectToTableConfig)
			if e != nil {
				return e
			}
		}
	}

	return nil
}

// DeleteData deletes data from table for specific client + dates
//
func (importer *Importer) deleteData(softwareClientLicense *creds.SoftwareClientLicense, importable Importable, config *ig.Config, bigQueryService *go_bigquery.Service, startDate *civil.Date, endDate *civil.Date) *errortools.Error {
	_bigQueryDataset := config.Dataset
	ig.WithEnvironment(&_bigQueryDataset)

	// delete old data
	sqlWhere := "1 = 1"
	if softwareClientLicense != nil {
		sqlWhere = fmt.Sprintf("SoftwareClientLicenseGuid_ = '%s'", softwareClientLicense.SoftwareClientLicenseGuid)
	}

	whereString := importable.Table().Replace.WhereString()
	if whereString == nil {
		if ig.IsEnvironmentTest() {
			fmt.Println("No delete filter")
		}
		return nil
	}

	sqlWhere = fmt.Sprintf("%s AND %s", sqlWhere, *whereString)

	if ig.IsEnvironmentTest() {
		fmt.Println("delete:", sqlWhere)
	}

	sqlConfig := go_bigquery.SqlConfig{
		DatasetName:     _bigQueryDataset,
		TableOrViewName: &importable.Table().Name,
		SqlWhere:        &sqlWhere,
	}

	e := bigQueryService.Delete(&sqlConfig)
	if e != nil {
		return e
	}

	return nil
}
