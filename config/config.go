package config

import (
	"log"
	"strings"

	"github.com/brianmcgraw/CFS-Geocoding/geocode"
	"github.com/brianmcgraw/CFS-Geocoding/mgo"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dyn "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	MGO_CREDS = "MGO_CREDS"
	MGO_HOST  = "MGO_HOST"
	AWSKEY    = "AWS_ACCESS_KEY_ID"
	AWSSECRET = "AWS_SECRET_ACCESS_KEY"
)

type Configuration struct {
	MgoUser    string
	MgoPW      string
	MgoHost    string
	MapsClient geocode.MapsConfig
	DynDB      *dyn.DynamoDB
	Mgo        *mongo.Client
}

// Add "InLambda" variable to grab envs vars conditionalls on whether or not its deployed

func New() (config Configuration) {
	CheckAWS()
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})

	if err != nil {
		log.Fatal("Unable to create AWS session, shutting down.")
	}

	svc := dynamodb.New(sess)
	config.DynDB = svc

	maps := geocode.NewClient()

	config.MapsClient = maps
	config.MgoHost = config.GetMgoHost()
	config.MgoUser, config.MgoPW = config.getMgoCreds()

	config.Mgo, err = mgo.Connect(config.MgoUser, config.MgoPW, config.MgoHost)

	if err != nil {
		log.Fatalf("Unable to connect to MongoDB: %v", err)
	}
	// add some credentials

	return config

}

func CheckAWS() {
	viper.BindEnv(AWSKEY)
	viper.BindEnv(AWSSECRET)
	if !viper.IsSet(AWSKEY) || !viper.IsSet(AWSSECRET) {
		log.Fatalf("Unable to retrieve AWS credentials environment variable")
	}
}

func (c Configuration) getMgoCreds() (a, b string) {
	viper.BindEnv(MGO_CREDS)
	if !viper.IsSet(MGO_CREDS) {
		log.Fatalf("Unable to retrieve mongo credentials environment variable")
	}

	creds := viper.GetString(MGO_CREDS)

	credsArr := strings.Split(creds, ":")

	if len(credsArr) != 2 {
		log.Fatalf("Credentials array not in proper format")
	}

	a = credsArr[0]
	b = credsArr[1]
	return a, b
}

func (c Configuration) GetMgoHost() string {
	viper.BindEnv(MGO_HOST)
	if !viper.IsSet(MGO_HOST) {
		log.Fatalf("Unable to retrieve mongo host environment variable")
	}

	return viper.GetString(MGO_HOST)
}
