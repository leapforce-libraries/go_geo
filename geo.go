package geo

import (
	"strings"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"

	"google.golang.org/api/iterator"
)

const (
	bigqueryDataSetGeo         string = "geo"
	bigqueryTablenameCountries string = "countries"
)

type Geo struct {
	BigQuery             *bigquerytools.BigQuery
	CountryAliases       []CountryAlias
	CountryCacheForID    map[string]string
	CountryCacheForAlias map[string]string
}

type CountryAlias struct {
	CountryId string
	Alias     string
	AliasType string
	Source    string
	Language  string
}

func (g *Geo) GetCountryAliases() error {
	sqlSelect := "CountryId, Alias, AliasType, IFNULL(Source,'') AS Source, IFNULL(Language,'') AS Language"
	sqlWhere := "CountryId IS NOT NULL AND Alias IS NOT NULL AND AliasType IS NOT NULL"

	it, err := g.BigQuery.Select(bigqueryDataSetGeo, bigqueryTablenameCountries, sqlSelect, sqlWhere, "")
	if err != nil {
		return err
	}

	for {
		var ca CountryAlias
		err := it.Next(&ca)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		g.CountryAliases = append(g.CountryAliases, ca)
	}

	return nil
}

// FindCountryAlias searches for CountryAlias matching the criteria
//
func (g *Geo) FindCountryAlias(countryId string, aliasType string, source string, language string) (string, error) {
	if countryId == "" {
		return "", nil
	}

	// get aliases if needed
	if len(g.CountryAliases) == 0 {
		err := g.GetCountryAliases()
		if err != nil {
			return "", err
		}
	}

	// init cache if needed
	if g.CountryCacheForAlias == nil {
		g.CountryCacheForAlias = make(map[string]string)
	}

	aliasType = strings.ToLower(aliasType)
	source = strings.ToLower(source)
	language = strings.ToLower(language)

	key := countryId + ";;" + aliasType + ";;" + source + ";;" + language
	alias, ok := g.CountryCacheForAlias[key]

	if ok {
		//fmt.Println("from cache:", alias)
		return alias, nil
	}

	alias = ""

	for _, ca := range g.CountryAliases {
		if strings.ToLower(ca.CountryId) == strings.ToLower(countryId) {
			if !strings.Contains(","+strings.ToLower(ca.AliasType)+",", ","+strings.ToLower(aliasType)+",") {
				continue
			}
			if !strings.Contains(","+strings.ToLower(ca.Language)+",", ","+strings.ToLower(language)+",") {
				continue
			}
			if !strings.Contains(","+strings.ToLower(ca.Source)+",", ","+strings.ToLower(source)+",") {
				continue
			}

			if alias != "" && alias != ca.Alias {
				alias = ""
				break
			}

			alias = ca.Alias
		}
	}

	if alias != "" {
		g.CountryCacheForAlias[key] = alias
		//fmt.Println("in cache:", alias)
	}

	return alias, nil
}

// FindCountryId searches for CountryAlias matching the comma-separated aliastypes, sources and languages
// and returns the CountryId
//
func (g *Geo) FindCountryId(input string, aliasTypes string, sources string, languages string) (string, error) {
	if input == "" {
		return "", nil
	}

	// get aliases if needed
	if len(g.CountryAliases) == 0 {
		err := g.GetCountryAliases()
		if err != nil {
			return "", err
		}
	}

	// init cache if needed
	if g.CountryCacheForID == nil {
		g.CountryCacheForID = make(map[string]string)
	}

	aliasTypes = strings.ToLower(aliasTypes)
	sources = strings.ToLower(sources)
	languages = strings.ToLower(languages)

	key := input + ";;" + aliasTypes + ";;" + sources + ";;" + languages
	id, ok := g.CountryCacheForID[key]

	if ok {
		//fmt.Println("from cache:", id)
		return id, nil
	}

	id = ""

	for _, ca := range g.CountryAliases {
		if aliasTypes != "" && !strings.Contains(","+strings.ToLower(aliasTypes)+",", ","+strings.ToLower(ca.AliasType)+",") {
			continue
		}
		if languages != "" && !strings.Contains(","+strings.ToLower(languages)+",", ","+strings.ToLower(ca.Language)+",") {
			continue
		}
		if sources != "" && !strings.Contains(","+strings.ToLower(sources)+",", ","+strings.ToLower(ca.Source)+",") {
			continue
		}
		if strings.ToLower(input) == strings.ToLower(ca.Alias) {
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
		g.CountryCacheForID[key] = id
		//fmt.Println("in cache:", id)
	}

	return id, nil
}

// ClearCountryCache clears cache with matched countries
//
func (g *Geo) ClearCountryCache() {
	g.CountryCacheForID = nil
}
