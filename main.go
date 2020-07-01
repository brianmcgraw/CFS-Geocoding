package main

import (
	"log"

	"github.com/brianmcgraw/CFS-Geocoding/dynamo"
	"github.com/brianmcgraw/CFS-Geocoding/geocode"
	"github.com/brianmcgraw/CFS-Geocoding/mgo"

	"github.com/brianmcgraw/CFS-Geocoding/config"
)

//

// Initial upload to CFS table, complete = false
// Daily (?) query from another lambda, pulling anything with complete = false;
// This list of addresses is geocoded, added to new table = cfsgeocode
// On success, patch anything in dynamoDB with that current address and set complete = true

// e.g., you have a list of 50 initial incomplete entries
// you hit google maps api with all 50, all success, patch every

type GeocodedAddressResponse struct {
	AddressComponents []AddressComponent `json:"address_components"`
}

type AddressComponent struct {
	LongName  string   `json:"long_name"`
	ShortName string   `json:"short_name"`
	Types     []string `json:"types"`
}

func main() {
	log.Println("Running CFS-Geocode...")
	config := config.New()

	// 1. Pull all incomplete values from the raw CFS table in dynamo.
	cfs, err := dynamo.QueryIncompleteCFS(config.DynDB) // This should return a complete record, but we're only interested in the incomplete part.

	if err != nil {
		log.Fatalf("Error querying the dynamoDB location table, exiting: %v", err)
	}

	errCounter := 0

	for _, value := range cfs {

		// 2. For each value returned from Dynamo, check and see if it exists in Mongo.
		locationResult, err := mgo.QueryLocationCFSMongo(value, config.Mgo) // Mongo will store the location data only, it matches on the Location (e.g., 4300 POTOMAC)

		if err != nil {
			errCounter += 1
			// to do , update this to exclude document not found
			log.Printf("Error querying the mgo location table: %v", err)
		}

		if locationResult.Location != "" {
			// The record exists in Mongo.
			patchRecordSuccess := checkGoodLocation(locationResult)
			if patchRecordSuccess {
				// 3. If Mongo had a good value, path the original DynamoDB record and move on
				updatedCfs := updateCFS(value, locationResult)
				err = dynamo.PatchRawDynamoSuccess(config.DynDB, updatedCfs)
				if err != nil {
					errCounter += 1
					log.Printf("Error marking issue with CFS record: %v", err)
				}

			} else {
				// The record exists in Mongo but has missing values.
				err = dynamo.PatchRawDynamoFailure(config.DynDB, value)
				if err != nil {
					errCounter += 1
					log.Printf("Error marking issue with CFS record: %v", err)
				}

				err = mgo.UpdateCFSMongoFailure(value, config.Mgo)

				if err != nil {
					errCounter += 1
					log.Printf("Error marking issue with mongo record: %v", err)
				}
			}

		} else {
			// The record did not exist in Mongo, so we get good values and update everything.
			// Function call to google maps: patch both data tables
			log.Println("did not find document in mongo, calling google maps api")
			improvedCfs, err := geocode.CallMaps(config.MapsClient, value)
			if err != nil {
				// if gmaps api error, patch hasissue = true in both places
				log.Printf("Error from google maps api: %v", err)
				err = dynamo.PatchRawDynamoFailure(config.DynDB, value)
				if err != nil {
					errCounter += 1
					log.Printf("Error marking issue with CFS record: %v", err)
				}
				err = mgo.UpdateCFSMongoFailure(value, config.Mgo)

				if err != nil {
					errCounter += 1
					log.Printf("Error marking issue with Mongo record: %v", err)
				}
				break
			}
			// Update both values successfully.
			// err = dynamo.PatchRawDynamoSuccess(config.DynDB, improvedCfs, locationResult)
			err = dynamo.PatchRawDynamoSuccess(config.DynDB, improvedCfs)

			if err != nil {
				errCounter += 1
				log.Printf("Error updating Raw dynamo record after success: %v", err)
			}
			log.Printf("Calling updateCFSMongoSuccess with value: %v", improvedCfs)
			err = mgo.UpdateCFSMongoSuccess(improvedCfs, config.Mgo)

			if err != nil {
				errCounter += 1
				log.Printf("Error updating Raw Mongo record after success: %v", err)
			}

		}

	}

	if (len(cfs)) == 0 {
		log.Println("No new items from CFS, shutting down.")
	}

	log.Printf("Successfully processed %v with %v errors.", len(cfs), errCounter)
	log.Println("Shutting down CFS-Geocode...")
}

func checkGoodLocation(lr dynamo.LocationFixed) bool {
	good := true

	if len(lr.LatLong.Lat) == 0 || lr.Ward == "" || lr.NeighborhoodLong == "" || lr.Zipcode == "" {
		good = false
	}

	return good
}

func updateCFS(cfs dynamo.CFS, locationResult dynamo.LocationFixed) dynamo.CFS {

	updatedCFS := dynamo.CFS{}

	updatedCFS.TimeOfCall = cfs.TimeOfCall
	updatedCFS.EventID = cfs.EventID
	updatedCFS.Location = cfs.Location
	updatedCFS.CallReason = cfs.CallReason
	updatedCFS.LatLong = locationResult.LatLong
	updatedCFS.Ward = locationResult.Ward
	updatedCFS.NeighborhoodLong = locationResult.NeighborhoodLong
	updatedCFS.NeighborhoodShort = locationResult.NeighborhoodShort
	updatedCFS.Zipcode = locationResult.Zipcode
	updatedCFS.Complete = cfs.Complete
	updatedCFS.HasIssue = cfs.HasIssue

	return updatedCFS
}
