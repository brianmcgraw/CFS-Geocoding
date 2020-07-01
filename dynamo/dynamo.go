package dynamo

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dyn "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	BaseDataTable          = "cfs-neww"
	FixedLocationDataTable = "cfs-location-fixed"
)

var boolPtrFalse = false
var boolPtrTrue = true

type CFS struct {
	TimeOfCall        time.Time `json:"time"`
	EventID           string    `json:"eventID"`
	Location          string    `json:"location"`
	CallReason        string    `json:"callReason"`
	LatLong           LatLong   `json:"latlong"`
	Ward              string    `json:"ward"`
	NeighborhoodLong  string    `json:"neighborhoodLong"`
	NeighborhoodShort string    `json:"neighborhoodShort"`
	Zipcode           string    `json:"zipcode"`
	Complete          bool      `json:"complete"`
	HasIssue          bool      `json:"hasIssue"`
}

type LocationFixed struct {
	Location          string  `json:"location"`
	LatLong           LatLong `json:"latlong"`
	Ward              string  `json:"ward"`
	NeighborhoodLong  string  `json:"neighborhoodLong"`
	NeighborhoodShort string  `json:"neighborhoodShort"`
	Zipcode           string  `json:"zipcode"`
	HasIssue          bool    `json:"hasIssue"`
}

type LatLong struct {
	Lat string `json:"lat"`
	Lng string `json:"lng"`
}

func QueryIncompleteCFS(session *dyn.DynamoDB) (cfs []CFS, err error) {
	log.Println("Querying cfs")
	input := &dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				BOOL: &boolPtrFalse,
			},
		},
		FilterExpression: aws.String("complete = :v1 and hasIssue = :v1"),
		TableName:        aws.String(BaseDataTable),
	}

	result, err := session.Scan(input)

	for _, v := range result.Items {
		var c CFS
		dynamodbattribute.UnmarshalMap(v, &c)
		cfs = append(cfs, c)
	}

	return cfs, err

}

func QueryLocationCFS(cfs CFS, session *dyn.DynamoDB) (locationResult LocationFixed, err error) {
	var locationResults []LocationFixed

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(cfs.Location),
			},
			":v2": {
				BOOL: &boolPtrFalse,
			},
		},
		KeyConditionExpression: aws.String("location = :v1 and hasIssue = :v2"),
		TableName:              aws.String(FixedLocationDataTable),
	}

	result, err := session.Query(input)

	// We are querying on a partion key so it should always return an array with at least 1 value
	for _, v := range result.Items {
		var temp LocationFixed
		dynamodbattribute.UnmarshalMap(v, &temp)

		locationResults = append(locationResults, temp)
	}

	if len(locationResults) > 0 {
		locationResult = locationResults[0]
	}

	return locationResult, err

}

func PatchRawDynamoFailure(session *dyn.DynamoDB, cfs CFS) (err error) {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#Y": aws.String("hasIssue"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":y": {
				BOOL: &boolPtrTrue,
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"eventID": {
				S: aws.String(cfs.EventID),
			},
		},
		TableName:        aws.String(BaseDataTable),
		UpdateExpression: aws.String("SET #Y = :y"),
	}

	_, err = session.UpdateItem(input)

	return err
}

func PatchRawDynamoSuccess(session *dyn.DynamoDB, cfs CFS) (err error) {
	// TODO, fix the handling of nexted object in dynamodb

	// log.Println(locationRecord)

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#a":  aws.String("latlong"),
			"#aa": aws.String("lat"),
			"#ab": aws.String("lng"),
			// "#b":  aws.String("ward"),
			"#c": aws.String("neighborhoodLong"),
			"#d": aws.String("neighborhoodShort"),
			"#e": aws.String("zipcode"),
			"#f": aws.String("complete"),
			"#g": aws.String("hasIssue"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":aa": {
				S: aws.String(cfs.LatLong.Lat),
			},
			":ab": {
				S: aws.String(cfs.LatLong.Lng),
			},
			// ":b": {
			// 	S: aws.String(cfs.Ward),
			// },
			":c": {
				S: aws.String(cfs.NeighborhoodLong),
			},
			":d": {
				S: aws.String(cfs.NeighborhoodShort),
			},
			":e": {
				S: aws.String(cfs.Zipcode),
			},
			":f": {
				BOOL: &boolPtrTrue,
			},
			":g": {
				BOOL: &boolPtrFalse,
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"eventID": {
				S: aws.String(cfs.EventID),
			},
		},
		TableName:        aws.String(BaseDataTable),
		UpdateExpression: aws.String("SET #a.#aa = :aa, #a.#ab = :ab, #c = :c, #d = :d, #e = :e, #f = :f, #g = :g"),
	}
	inputErr := input.Validate()

	log.Println(inputErr)

	output, err := session.UpdateItem(input)

	log.Println(output)

	log.Println(err)

	return err
}

// LatLong      []string  `json:"latlong"`
// 	Ward         string    `json:"ward"`
// 	Neighborhood string    `json:"neighborhood"`
// 	Zipcode      string    `json:"zipcode"`
// 	Complete     bool      `json:"complete"`
// 	HasIssue     bool      `json:"hasIssue"`
