package athena

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

const (
	QUERY_POLLING_INTERVAL = 1
	RUNNING                = "RUNNING"
	SUCCEEDED              = "SUCCEEDED"
	PROTOCOL               = "s3://"
)

// InputParams ...
type InputParams struct {
	AwsSession   *session.Session
	Database     *string
	QueryString  *string
	Region       *string
	OutputBucket *string
	AthenaClient *athena.Athena
}

// Query ...
type Query struct {
	inputParams *InputParams
	startInput  athena.StartQueryExecutionInput
	startOutput *athena.StartQueryExecutionOutput
	getInput    athena.GetQueryExecutionInput
	getOutput   *athena.GetQueryExecutionOutput
	results     *athena.GetQueryResultsOutput
}

// QueryAthena ...
//
// Queries AWS Athena and returns back a result set
func QueryAthena(inputParams *InputParams) ([]*athena.Row, error) {

	var err error
	var q Query
	q.inputParams = inputParams

	// build query
	q.buildQuery()

	// get client
	q.getClient()

	// start query
	err = q.startQuery()
	if err != nil {
		return nil, err
	}

	// wait for query to process
	err = q.pollOutput()
	if err != nil {
		return nil, err
	}

	// handle failure
	err = q.handleFailure()
	if err != nil {
		return nil, err
	}

	// handle success
	err = q.handleSuccess()
	if err != nil {
		return nil, err
	}

	return q.results.ResultSet.Rows, nil

}

func (q *Query) buildQuery() {

	// set query string
	q.startInput.SetQueryString(*q.inputParams.QueryString)

	// set context
	var queryExecutionContext athena.QueryExecutionContext
	queryExecutionContext.SetDatabase(*q.inputParams.Database)
	q.startInput.SetQueryExecutionContext(&queryExecutionContext)

	// set result configuration
	var resultConfig athena.ResultConfiguration
	resultConfig.SetOutputLocation(PROTOCOL + *q.inputParams.OutputBucket)
	q.startInput.SetResultConfiguration(&resultConfig)

}

func (q *Query) getClient() {

	if q.inputParams.AwsSession == nil {

		// configure AWS session
		awsConfig := &aws.Config{}
		awsConfig.WithRegion(*q.inputParams.Region)

		// start AWS session
		q.inputParams.AwsSession = session.Must(session.NewSession(awsConfig))

	}

	if q.inputParams.AthenaClient == nil {

		// instantiate Athena client
		q.inputParams.AthenaClient = athena.New(q.inputParams.AwsSession, aws.NewConfig().WithRegion(*q.inputParams.Region))

	}

}

func (q *Query) startQuery() error {
	var err error
	q.startOutput, err = q.inputParams.AthenaClient.StartQueryExecution(&q.startInput)
	if err != nil {
		return err
	}
	return nil
}

func (q *Query) pollOutput() error {

	q.getInput.SetQueryExecutionId(*q.startOutput.QueryExecutionId)

	var err error

	for {
		q.getOutput, err = q.inputParams.AthenaClient.GetQueryExecution(&q.getInput)
		if err != nil {
			return err
		}

		// check if query is running
		if *q.getOutput.QueryExecution.Status.State != RUNNING {
			return nil
		}

		// rest between iterations
		time.Sleep(QUERY_POLLING_INTERVAL * time.Second)

	}

}

func (q *Query) handleFailure() error {
	if *q.getOutput.QueryExecution.Status.State != SUCCEEDED {
		return errors.New("Query failure: " + *q.getOutput.QueryExecution.Status.State)
	}
	return nil
}

func (q *Query) handleSuccess() error {
	var getQueryResultsInput athena.GetQueryResultsInput
	getQueryResultsInput.SetQueryExecutionId(*q.startOutput.QueryExecutionId)
	var err error
	q.results, err = q.inputParams.AthenaClient.GetQueryResults(&getQueryResultsInput)
	if err != nil {
		return err
	}
	return nil
}
