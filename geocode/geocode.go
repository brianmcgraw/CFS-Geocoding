package geocode

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/brianmcgraw/CFS-Geocoding/dynamo"
)

const (
	GMAPSAPIKEY = "Maps_API_Key"
	MAPSURL     = "https://maps.googleapis.com/maps/api/geocode/json"
)

type MapsConfig struct {
	Client *http.Client
	ApiKey string
	URL    *url.URL
}

type MapsResponse struct {
	Results []MapsResults `json:"results"`
}

type MapsResults struct {
	AddressComponents []AddressComponent `json:"address_components"`
	FormattedAddress  string             `json:"formatted_address"`
	Geometry          MapsGeometry       `json:"geometry"`
	PlaceId           string             `json:"place_id"`
	Types             []string           `json:"types"`
}

type AddressComponent struct {
	LongName  string   `json:"long_name"`
	ShortName string   `json:"short_name"`
	Types     []string `json:"types"`
}

type MapsGeometry struct {
	Location GoogleLatLong `json:"location"`
}

type GoogleLatLong struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

func NewClient() MapsConfig {

	config := MapsConfig{}

	api_key := os.Getenv(GMAPSAPIKEY)

	if api_key == "" {
		log.Fatal("Maps API Key not found, shutting down.")
	}

	config.ApiKey = api_key
	config.Client = &http.Client{}
	url, err := url.Parse(MAPSURL)

	if err != nil {
		log.Printf("Bad value provided for google maps API: %v", err)
	}

	config.URL = url

	return config
}

func CallMaps(mapsConfig MapsConfig, raw dynamo.CFS) (improved dynamo.CFS, err error) {

	normalizedAddress := NormalizeAddress(raw.Location)
	url := buildURL(mapsConfig.URL, normalizedAddress, mapsConfig.ApiKey)
	log.Println("Calling google maps with: ", url.String())
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)

	if err != nil {
		log.Printf("Err building http request for google maps: %v", err)
	}

	resp, err := mapsConfig.Client.Do(req)

	if err != nil {
		log.Println("Error from google maps API")
		//TODO FIGURE out how tohandle this error
	}
	var mapsResponse MapsResponse

	// Turn these responses into errors
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Error calling google maps api, status code %v received", resp.StatusCode)
		return improved, err
	}

	err = json.NewDecoder(resp.Body).Decode(&mapsResponse)

	log.Println(mapsResponse)

	if err != nil {
		err = fmt.Errorf("Error decoding google maps api response: %v", err)
		return improved, err
	}

	for _, value := range mapsResponse.Results[0].AddressComponents {
		if checkContains(value.Types, "neighborhood") {
			improved.NeighborhoodLong = value.LongName
			improved.NeighborhoodShort = value.ShortName
		}

		if checkContains(value.Types, "postal_code") {
			improved.Zipcode = value.LongName
		}

	}

	latFloat := strconv.FormatFloat(mapsResponse.Results[0].Geometry.Location.Lat, 'f', 3, 32)
	lngFloat := strconv.FormatFloat(mapsResponse.Results[0].Geometry.Location.Lng, 'f', 3, 32)

	improved.LatLong.Lat = latFloat
	improved.LatLong.Lng = lngFloat
	improved.TimeOfCall = raw.TimeOfCall
	improved.EventID = raw.EventID
	improved.Location = raw.Location
	improved.CallReason = raw.CallReason

	log.Println(improved, " is the value")
	return improved, err

}

func NormalizeAddress(s string) (f string) {
	f = strings.ReplaceAll(s, "X", "0")
	return f
}

func buildURL(url *url.URL, s string, k string) *url.URL {
	q := url.Query()
	fixedString := s + "+SAINT+LOUIS+MO"
	q.Set("address", fixedString)
	q.Set("key", k)
	url.RawQuery = q.Encode()
	return url
}

func checkContains(arrayToCheck []string, valuetoCheck string) bool {
	for _, v := range arrayToCheck {
		if v == valuetoCheck {
			return true
		}
	}

	return false
}
