package geo

import (
	"strings"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	errortools "github.com/leapforce-libraries/go_errortools"

	"google.golang.org/api/iterator"
)

const (
	bigqueryDataSetGeo         string = "geo"
	bigqueryTablenameCountries string = "countries"
)

type Geo struct {
	bigQuery             *bigquerytools.BigQuery
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

func NewGeo(bq *bigquerytools.BigQuery) (*Geo, *errortools.Error) {
	if bq == nil {
		return nil, errortools.ErrorMessage("BigQuery object passed to NewGeo may not be a nil pointer.")
	}

	return &Geo{
		bigQuery: bq,
	}, nil
}

func (g *Geo) getCountryAliases() *errortools.Error {
	sqlSelect := "CountryId, Alias, AliasType, IFNULL(Source,'') AS Source, IFNULL(Language,'') AS Language"
	sqlWhere := "CountryId IS NOT NULL AND Alias IS NOT NULL AND AliasType IS NOT NULL"

	it, e := g.bigQuery.Select(bigqueryDataSetGeo, bigqueryTablenameCountries, sqlSelect, sqlWhere, "")
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

		g.countryAliases = append(g.countryAliases, ca)
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
func (g *Geo) CountryID2CountryAlias(countryId string, filter *CountryAliasFilter) (string, *errortools.Error) {
	if countryId == "" {
		return "", nil
	}

	// get aliases if needed
	if len(g.countryAliases) == 0 {
		e := g.getCountryAliases()
		if e != nil {
			return "", e
		}
	}

	// init cache if needed
	if g.countryCacheForAlias == nil {
		g.countryCacheForAlias = make(map[string]string)
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
	alias, ok := g.countryCacheForAlias[key]

	if ok {
		//fmt.Println("from cache:", alias)
		return alias, nil
	}

	alias = ""

	for _, ca := range g.countryAliases {
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
		g.countryCacheForAlias[key] = alias
		//fmt.Println("in cache:", alias)
	}

	return alias, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (g *Geo) CountryAlias2CountryID(alias string, filter *CountryAliasFilter) (string, *errortools.Error) {
	if alias == "" {
		return "", nil
	}

	// get aliases if needed
	if len(g.countryAliases) == 0 {
		e := g.getCountryAliases()
		if e != nil {
			return "", e
		}
	}

	// init cache if needed
	if g.countryCacheForID == nil {
		g.countryCacheForID = make(map[string]string)
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
	id, ok := g.countryCacheForID[key]

	if ok {
		//fmt.Println("from cache:", id)
		return id, nil
	}

	id = ""

	for _, ca := range g.countryAliases {
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
		g.countryCacheForID[key] = id
		//fmt.Println("in cache:", id)
	}

	return id, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (g *Geo) CountryAlias2CountryAlias(aliasFrom string, filterFrom *CountryAliasFilter, filterTo *CountryAliasFilter) (string, *errortools.Error) {
	countryID, e := g.CountryAlias2CountryID(aliasFrom, filterFrom)
	if e != nil {
		return "", e
	}

	if countryID == "" {
		return "", nil
	}

	aliasTo, e := g.CountryID2CountryAlias(countryID, filterTo)
	if e != nil {
		return "", e
	}

	return aliasTo, nil
}

// clearCountryCache clears cache with matched countries
//
func (g *Geo) clearCountryCache() {
	g.countryCacheForID = nil
}
