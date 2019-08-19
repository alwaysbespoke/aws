package ssm

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
)

// GetParam ...
func GetParam(key *string, withDecryption bool) (*string, error) {

	// vars
	var (
		awsSession *session.Session
		param      *ssm.GetParameterOutput
		err        error
	)

	// set region
	region := os.Getenv("AWS_Region")
	if len(region) == 0 {
		region = "us-east-1"
	}

	// start AWS session
	awsSession, err = session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(region)},
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	// call parameter store
	svc := ssm.New(awsSession, aws.NewConfig().WithRegion(region))
	param, err = svc.GetParameter(&ssm.GetParameterInput{
		Name:           key,
		WithDecryption: &withDecryption,
	})
	if err != nil {
		return nil, err
	}

	return param.Parameter.Value, nil

}
