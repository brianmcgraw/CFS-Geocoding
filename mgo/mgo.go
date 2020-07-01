package mgo

import (
	"context"
	"log"

	"github.com/brianmcgraw/CFS-Geocoding/dynamo"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Connect(user string, pw string, host string) (client *mongo.Client, err error) {

	mongoClient := options.Client()
	creds := options.Credential{AuthMechanism: "SCRAM-SHA-1",
		Username: user, Password: pw}
	mongoClient.SetAuth(creds)

	ctx := context.TODO()

	log.Println("Attempting to connect to Mongo...")
	client, err = mongo.Connect(ctx, mongoClient.ApplyURI(host))

	if err != nil {
		log.Fatalf("Error while attempting to connect to Mongo DB: %v", err)

	}
	log.Println("Connected to mongo")
	log.Println("pinging mongo")
	err = client.Ping(ctx, nil)

	if err != nil {
		log.Fatalf("Unable to ping Mongo")
	}
	log.Println("pinged mongo")

	return client, err
}

func QueryLocationCFSMongo(cfs dynamo.CFS, client *mongo.Client) (locationResult dynamo.LocationFixed, err error) {
	collection := client.Database("cfs").Collection("locationcfs")

	var locationResults dynamo.LocationFixed
	query := bson.M{
		"location": bson.M{"$eq": cfs.Location},
		"hasIssue": bson.M{"$eq": false},
	}

	log.Println(query)
	log.Println(cfs.Location)

	result := collection.FindOne(context.TODO(), query)

	err = result.Decode(locationResults)
	return locationResults, err
}

func UpdateCFSMongoFailure(cfs dynamo.CFS, client *mongo.Client) (err error) {
	collection := client.Database("cfs").Collection("locationcfs")

	opts := options.Update().SetUpsert(true)
	query := bson.M{
		"location": bson.M{"$eq": cfs.Location},
	}
	update := bson.M{"$set": bson.M{"hasIssue": true}}

	_, err = collection.UpdateOne(context.TODO(), query, update, opts)

	return err
}

func UpdateCFSMongoSuccess(cfs dynamo.CFS, client *mongo.Client) (err error) {
	collection := client.Database("cfs").Collection("locationcfs")

	opts := options.Update().SetUpsert(true)
	query := bson.M{
		"location": bson.M{"$eq": cfs.Location},
	}
	update := bson.M{"$set": bson.M{
		"latlong":           cfs.LatLong,
		"neighborhoodLong":  cfs.NeighborhoodLong,
		"neighborhoodShort": cfs.NeighborhoodShort,
		"zipcode":           cfs.Zipcode,
		"hasIssue":          false}}

	_, err = collection.UpdateOne(context.TODO(), query, update, opts)

	return err
}
