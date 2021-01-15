package geo

import (
	"strings"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"

	"google.golang.org/api/iterator"
)

const (
	bigQueryDataSetGeo         string = "geo"
	bigQueryTablenameCountries string = "countries"
)

type Service struct {
	bigQueryService      *bigquery.Service
	countryAliases       []CountryAlias
	countryCacheForID    map[string]string
	countryCacheForAlias map[string]string
}

type CountryAlias struct {
	CountryId string
	Alias     string
	AliasType string
	Source    string
	Language  string
}

func NewService(bigQueryService *bigquery.Service) (*Service, *errortools.Error) {
	if bigQueryService == nil {
		return nil, errortools.ErrorMessage("BigQuery object passed to NewService may not be a nil pointer.")
	}

	return &Service{
		bigQueryService: bigQueryService,
	}, nil
}

func (service *Service) getCountryAliases() *errortools.Error {
	sqlSelect := "CountryId, Alias, AliasType, IFNULL(Source,'') AS Source, IFNULL(Language,'') AS Language"
	sqlWhere := "CountryId IS NOT NULL AND Alias IS NOT NULL AND AliasType IS NOT NULL"

	it, e := service.bigQueryService.Select(bigQueryDataSetGeo, bigQueryTablenameCountries, sqlSelect, sqlWhere, "")
	if e != nil {
		return e
	}

	for {
		var ca CountryAlias
		err := it.Next(&ca)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errortools.ErrorMessage(e)
		}

		service.countryAliases = append(service.countryAliases, ca)
	}

	return nil
}

type CountryAliasFilter struct {
	AliasType *string
	Source    *string
	Language  *string
}

// FindCountryAlias searches for CountryAlias matching the criteria
//
func (service *Service) CountryID2CountryAlias(countryId string, filter *CountryAliasFilter) (string, *errortools.Error) {
	if countryId == "" {
		return "", nil
	}

	// get aliases if needed
	if len(service.countryAliases) == 0 {
		e := service.getCountryAliases()
		if e != nil {
			return "", e
		}
	}

	// init cache if needed
	if service.countryCacheForAlias == nil {
		service.countryCacheForAlias = make(map[string]string)
	}

	aliasType := ""
	source := ""
	language := ""

	if filter != nil {
		if filter.AliasType != nil {
			aliasType = strings.ToLower(*filter.AliasType)
		}

		if filter.Source != nil {
			source = strings.ToLower(*filter.Source)
		}

		if filter.Language != nil {
			language = strings.ToLower(*filter.Language)
		}
	}

	key := countryId + ";;" + aliasType + ";;" + source + ";;" + language
	alias, ok := service.countryCacheForAlias[key]

	if ok {
		//fmt.Println("from cache:", alias)
		return alias, nil
	}

	alias = ""

	for _, ca := range service.countryAliases {
		if strings.ToLower(ca.CountryId) == strings.ToLower(countryId) {
			if aliasType != "" {
				if !strings.Contains(","+strings.ToLower(ca.AliasType)+",", ","+strings.ToLower(aliasType)+",") {
					continue
				}
			}
			if source != "" {
				if !strings.Contains(","+strings.ToLower(ca.Source)+",", ","+strings.ToLower(source)+",") {
					continue
				}
			}
			if language != "" {
				if !strings.Contains(","+strings.ToLower(ca.Language)+",", ","+strings.ToLower(language)+",") {
					continue
				}
			}

			if alias != "" && alias != ca.Alias {
				alias = ""
				break
			}

			alias = ca.Alias
		}
	}

	if alias != "" {
		service.countryCacheForAlias[key] = alias
		//fmt.Println("in cache:", alias)
	}

	return alias, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (service *Service) CountryAlias2CountryID(alias string, filter *CountryAliasFilter) (string, *errortools.Error) {
	if alias == "" {
		return "", nil
	}

	// get aliases if needed
	if len(service.countryAliases) == 0 {
		e := service.getCountryAliases()
		if e != nil {
			return "", e
		}
	}

	// init cache if needed
	if service.countryCacheForID == nil {
		service.countryCacheForID = make(map[string]string)
	}

	aliasType := ""
	source := ""
	language := ""

	if filter != nil {
		if filter.AliasType != nil {
			aliasType = strings.ToLower(*filter.AliasType)
		}

		if filter.Source != nil {
			source = strings.ToLower(*filter.Source)
		}

		if filter.Language != nil {
			language = strings.ToLower(*filter.Language)
		}
	}

	key := alias + ";;" + aliasType + ";;" + source + ";;" + language
	id, ok := service.countryCacheForID[key]

	if ok {
		//fmt.Println("from cache:", id)
		return id, nil
	}

	id = ""

	for _, ca := range service.countryAliases {
		if aliasType != "" {
			if aliasType != "" && !strings.Contains(","+strings.ToLower(aliasType)+",", ","+strings.ToLower(ca.AliasType)+",") {
				continue
			}
		}
		if source != "" {
			if source != "" && !strings.Contains(","+strings.ToLower(source)+",", ","+strings.ToLower(ca.Source)+",") {
				continue
			}
		}
		if language != "" {
			if language != "" && !strings.Contains(","+strings.ToLower(language)+",", ","+strings.ToLower(ca.Language)+",") {
				continue
			}
		}
		if strings.ToLower(alias) == strings.ToLower(ca.Alias) {
			if id != "" && id != ca.CountryId {
				// double match
				//fmt.Println("double!", id, ca.CountryId)
				id = ""
				break
			}
			id = ca.CountryId
		}
	}

	if id != "" {
		service.countryCacheForID[key] = id
		//fmt.Println("in cache:", id)
	}

	return id, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (service *Service) CountryAlias2CountryAlias(aliasFrom string, filterFrom *CountryAliasFilter, filterTo *CountryAliasFilter) (string, *errortools.Error) {
	countryID, e := service.CountryAlias2CountryID(aliasFrom, filterFrom)
	if e != nil {
		return "", e
	}

	if countryID == "" {
		return "", nil
	}

	aliasTo, e := service.CountryID2CountryAlias(countryID, filterTo)
	if e != nil {
		return "", e
	}

	return aliasTo, nil
}

// clearCountryCache clears cache with matched countries
//
func (service *Service) clearCountryCache() {
	service.countryCacheForID = nil
}
