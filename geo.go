package geo

import (
	"strings"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"

	"google.golang.org/api/iterator"
)

type Geo struct {
	CountryAliases             []CountryAlias
	BigQuery                   *bigquerytools.BigQuery
	BigQueryDataset            string
	BigQueryTablenameCountries string
	CountryCache               map[string]string
}

type CountryAlias struct {
	CountryId string
	Alias     string
	AliasType string
	Source    string
	Language  string
}

func (g *Geo) GetCountryAliases() error {
	sqlSelect := "CountryId, Alias, AliasType, IFNULL(Source,'') AS Source, Language"
	sqlWhere := "CountryId IS NOT NULL AND Alias IS NOT NULL AND AliasType IS NOT NULL AND Language IS NOT NULL"

	it, err := g.BigQuery.Get(g.BigQueryDataset, g.BigQueryTablenameCountries, sqlSelect, sqlWhere, "")
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

	/*
		ca := CountryAlias{}
		err := g.BigQuery.FillSlice(BIGQUERY_DATASET, BIGQUERY_TABLENAME_COUNTRIES, sqlSelect, sqlWhere, &countryAliases, ca)
		if err != nil {
			return err
		}

		g.CountryAliases = make([]CountryAlias, len(countryAliases))
		for i, a := range countryAliases {
			alias, ok := a.(CountryAlias)
			if ok {
				g.CountryAliases[i] = alias
			}
		}*/

	return nil
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
	if g.CountryCache == nil {
		g.CountryCache = make(map[string]string)
	}

	aliasTypes = strings.ToLower(aliasTypes)
	sources = strings.ToLower(sources)
	languages = strings.ToLower(languages)

	key := input + ";;" + aliasTypes + ";;" + sources + ";;" + languages
	id, ok := g.CountryCache[key]

	if ok {
		//fmt.Println("from cache:", id)
		return id, nil
	}

	id = ""

	for _, ca := range g.CountryAliases {
		if aliasTypes != "" && !strings.Contains(","+aliasTypes+",", ","+strings.ToLower(ca.AliasType)+",") {
			continue
		}
		if languages != "" && !strings.Contains(","+languages+",", ","+strings.ToLower(ca.Language)+",") {
			continue
		}
		if sources != "" && !strings.Contains(","+sources+",", ","+strings.ToLower(ca.Source)+",") {
			continue
		}
		if input == ca.Alias {
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
		g.CountryCache[key] = id
		//fmt.Println("in cache:", id)
	}

	return id, nil
}

// ClearCountryCache clears cache with matched countries
//
func (g *Geo) ClearCountryCache() {
	g.CountryCache = nil
}
