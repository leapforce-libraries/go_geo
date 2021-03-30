package geo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	googlemaps "github.com/leapforce-libraries/go_googlemaps"
)

type GeoCode struct {
	Address  string
	GeoCodes []byte
}

var newGeoCodes []*GeoCode
var currentGeoCodes map[string]*[]googlemaps.GeoCode

// clearCountryCache clears cache with matched countries
//
func (service *Service) GetGoogleMapsGeoCode(geoCodingService *googlemaps.GeoCodingService, address string) (*[]googlemaps.GeoCode, *errortools.Error) {
	geoCodeParams := googlemaps.GeoCodeParams{
		Address: address,
	}

	if currentGeoCodes == nil {
		currentGeoCodes = make(map[string]*[]googlemaps.GeoCode)
	}
	var geoCode *[]googlemaps.GeoCode = nil

	// lookup address in local memory
	geoCodeLocal, ok := currentGeoCodes[address]
	if ok {
		geoCode = geoCodeLocal
	}

	if geoCode == nil {
		// lookup address in BQ table
		_geoCode := GeoCode{}

		tableName := bigQueryTablenameGoogleGeoCodes
		sqlSelect := "Address, GeoCodes"
		sqlWhere := fmt.Sprintf("Address = \"%s\"", address)
		sqlConfig := bigquery.SQLConfig{
			DatasetName:     bigQueryDataSetGeo,
			TableOrViewName: &tableName,
			SQLSelect:       &sqlSelect,
			SQLWhere:        &sqlWhere,
		}

		rowCount, e := service.bigQueryService.GetStruct(&sqlConfig, &_geoCode)
		if e != nil {
			errortools.CaptureError(e)
		} else if rowCount > 0 {
			if rowCount > 1 {
				errortools.CaptureWarning(fmt.Sprintf("More than one GeoCode for address \"%s\" in table", address))
			}

			err := json.Unmarshal(_geoCode.GeoCodes, &geoCode)
			if err != nil {
				errortools.CaptureError(err)
			}

			//fmt.Printf("Address \"%s\" found in table\n", address)
		}
	}

	if geoCode == nil {
		// query address in GeoCoding API
		gc, e := geoCodingService.GeoCode(&geoCodeParams)
		if e != nil {
			return nil, e
		}
		if gc == nil {
			return nil, errortools.ErrorMessage("GetGoogleMapsGeoCode returned nil")
		}
		geoCode = gc

		_json, err := json.Marshal(geoCode)
		if e != nil {
			return nil, errortools.ErrorMessage(err)
		}

		newGeoCodes = append(newGeoCodes, &GeoCode{
			Address:  address,
			GeoCodes: _json,
		})
	}

	currentGeoCodes[address] = geoCode
	return geoCode, nil
}

func (service *Service) SaveNewGoogleMapsGeoCodes() *errortools.Error {
	if service.storageClient == nil {
		return errortools.ErrorMessage("Geo service was not initialized with google cloud storage client")
	}

	if len(newGeoCodes) == 0 {
		return nil
	} else {
		fmt.Printf("Saving %v addresses to geocode table\n", len(newGeoCodes))
	}

	ctx := context.Background()

	// create object handle
	objectName := fmt.Sprintf("geocode_%s", time.Now().Format("20060102_150405000"))
	obj := service.storageClient.Bucket(bucketName).Object(objectName)

	w := obj.NewWriter(ctx)

	for _, geoCode := range newGeoCodes {
		b, err := json.Marshal(*geoCode)
		if err != nil {
			return errortools.ErrorMessage(err)
		}

		// Write GeoCode
		if _, err := w.Write(b); err != nil {
			return errortools.ErrorMessage(err)
		}

		// Write NewLine
		if _, err := fmt.Fprintf(w, "\n"); err != nil {
			return errortools.ErrorMessage(err)
		}
	}

	// Close
	if err := w.Close(); err != nil {
		return errortools.ErrorMessage(err)
	}

	// copy data to BigQuery temp table
	tableName := bigQueryTablenameGoogleGeoCodes
	sqlConfig := bigquery.SQLConfig{
		DatasetName:     bigQueryDataSetGeo,
		TableOrViewName: &tableName,
		ModelOrSchema:   GeoCode{},
	}.GenerateTempTable()

	copyObjectToTableConfig := bigquery.CopyObjectToTableConfig{
		ObjectHandle:  obj,
		SQLConfig:     &sqlConfig,
		TruncateTable: false,
		DeleteObject:  true,
	}

	e := service.bigQueryService.CopyObjectToTable(&copyObjectToTableConfig)
	if e != nil {
		return e
	}

	// empty slice
	newGeoCodes = []*GeoCode{}

	return nil

}
