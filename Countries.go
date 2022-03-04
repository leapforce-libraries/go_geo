package geo

import (
	"sort"
	"strings"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	go_fuzzy "github.com/lithammer/fuzzysearch/fuzzy"
)

type CountryAlias struct {
	CountryId string
	Alias     string
	AliasType string
	Source    string
	Language  string
}

func (service *Service) getCountryAliases() *errortools.Error {
	tableName := bigQueryTablenameCountries
	sqlSelect := "CountryId, Alias, AliasType, IFNULL(Source,'') AS Source, IFNULL(Language,'') AS Language"
	sqlWhere := "CountryId IS NOT NULL AND Alias IS NOT NULL AND AliasType IS NOT NULL"
	sqlConfig := bigquery.SqlConfig{
		DatasetName:     bigQueryDataSetGeo,
		TableOrViewName: &tableName,
		SqlSelect:       &sqlSelect,
		SqlWhere:        &sqlWhere,
	}

	return service.bigQueryService.Select(&sqlConfig, &service.countryAliases)
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
		return alias, nil
	}

	alias = ""

	for _, ca := range service.countryAliases {
		if strings.EqualFold(ca.CountryId, countryId) {
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
	}

	return alias, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (service *Service) CountryAlias2CountryID(alias string, filter *CountryAliasFilter, fuzzy bool) (string, *errortools.Error) {
	alias = strings.Trim(alias, " ")

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
	if fuzzy && service.countryCacheForIDFuzzy == nil {
		service.countryCacheForIDFuzzy = make(map[string]string)
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

	id, e := service.matchCountry(alias, aliasType, source, language)
	if e != nil {
		return "", e
	}

	if id != "" {
		return id, nil
	}

	if fuzzy {
		return service.matchCountryFuzzy(alias, aliasType, source, language)
	}

	return id, nil
}

func (service *Service) matchCountryFuzzy(alias string, aliasType string, source string, language string) (string, *errortools.Error) {
	key := alias + ";;" + aliasType + ";;" + source + ";;" + language
	id, ok := service.countryCacheForIDFuzzy[key]

	if ok {
		return id, nil
	}

	id = ""

	aliases := []string{}

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

		aliases = append(aliases, ca.Alias)
	}

	if len(aliases) == 0 {
		return "", nil
	}

	matches := go_fuzzy.RankFindNormalizedFold(alias, aliases)

	if matches.Len() == 0 {
		return "", nil
	}

	sort.Sort(matches)
	alias = matches[0].Target

	// do non fuzzy matching to retrieve id (not optimal, but...)
	id, e := service.matchCountry(alias, aliasType, source, language)
	if e != nil {
		return "", e
	}

	if id != "" {
		service.countryCacheForIDFuzzy[key] = id
	}

	return id, nil
}

func (service *Service) matchCountry(alias string, aliasType string, source string, language string) (string, *errortools.Error) {
	key := alias + ";;" + aliasType + ";;" + source + ";;" + language
	id, ok := service.countryCacheForID[key]

	if ok {
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
		if strings.EqualFold(alias, ca.Alias) {
			if id != "" && id != ca.CountryId {
				// double match
				id = ""
				break
			}
			id = ca.CountryId
		}
	}

	if id != "" {
		service.countryCacheForID[key] = id
	}

	return id, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (service *Service) CountryAlias2CountryAlias(aliasFrom string, filterFrom *CountryAliasFilter, filterTo *CountryAliasFilter, fuzzy bool) (string, *errortools.Error) {
	countryID, e := service.CountryAlias2CountryID(aliasFrom, filterFrom, fuzzy)
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
