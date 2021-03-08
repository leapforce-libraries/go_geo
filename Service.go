package geo

import (
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
)

const (
	bigQueryDataSetGeo              string = "geo"
	bigQueryTablenameCountries      string = "countries"
	bigQueryTablenameGoogleGeoCodes string = "google_geocodes"
	bucketName                      string = "leapforce_gm_data"
)

type Service struct {
	bigQueryService      *bigquery.Service
	storageClient        *storage.Client
	countryAliases       []CountryAlias
	countryCacheForID    map[string]string
	countryCacheForAlias map[string]string
}

func NewService(bigQueryService *bigquery.Service, storageClient *storage.Client) (*Service, *errortools.Error) {
	if bigQueryService == nil {
		return nil, errortools.ErrorMessage("BigQuery object passed to NewService may not be a nil pointer.")
	}

	return &Service{
		bigQueryService: bigQueryService,
		storageClient:   storageClient,
	}, nil
}
