package dms

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
)

const (
	T2_MICRO   = "dms.t2.micro"
	T2_SMALL   = "dms.t2.small"
	T2_MEDIUM  = "dms.t2.medium"
	T2_LARGE   = "dms.t2.large"
	C4_LARGE   = "dms.c4.large"
	C4_XLARGE  = "dms.c4.xlarge"
	C4_2XLARGE = "dms.c4.2xlarge"
	C4_4XLARGE = "dms.c4.4xlarge"
)

// ModifyReplicationInstanceClass ...
//
// Todo
func ModifyReplicationInstanceClass(awsSession *session.Session, arn string, class string) error {
	svc := databasemigrationservice.New(awsSession)
	input := &databasemigrationservice.ModifyReplicationInstanceInput{
		ApplyImmediately:         aws.Bool(true),
		ReplicationInstanceArn:   aws.String(arn),
		ReplicationInstanceClass: aws.String(class),
	}
	switch class {
	case T2_MICRO:
	case T2_SMALL:
	case T2_MEDIUM:
	case T2_LARGE:
	case C4_LARGE:
	case C4_XLARGE:
	case C4_2XLARGE:
	case C4_4XLARGE:
	default:
		return errors.New("Invalid instance class: " + class)
	}
	_, err := svc.ModifyReplicationInstance(input)
	if err != nil {
		return err
	}
	return nil
}

// CheckReplicationInstanceClassAndStatus ...
//
// Todo
func CheckReplicationInstanceClassAndStatus(awsSession *session.Session, arn string, class string) error {
	svc := databasemigrationservice.New(awsSession)
	input := &databasemigrationservice.DescribeReplicationInstancesInput{}
	results, err := svc.DescribeReplicationInstances(input)
	if err != nil {
		return err
	}
	for _, replicationInstance := range results.ReplicationInstances {
		if *replicationInstance.ReplicationInstanceArn == arn {
			if *replicationInstance.ReplicationInstanceClass != class {
				return errors.New("Current class does not match expected: current: " + *replicationInstance.ReplicationInstanceClass + ", expected: " + class)
			}
			if *replicationInstance.ReplicationInstanceStatus != "available" {
				return errors.New("Replication instance not available: status: " + *replicationInstance.ReplicationInstanceStatus)
			}
			return nil
		}
	}
	return errors.New("Instance not found: " + arn)
}

// StartReplicationTask ...
//
// Todo
func StartReplicationTask(awsSession *session.Session, arn string) error {
	svc := databasemigrationservice.New(awsSession)
	input := &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
		StartReplicationTaskType: aws.String("start-replication"),
	}
	_, err := svc.StartReplicationTask(input)
	if err != nil {
		return err
	}
	return nil
}

// ResumeReplicationTask ...
//
// Todo
func ResumeReplicationTask(awsSession *session.Session, arn string) error {
	svc := databasemigrationservice.New(awsSession)
	input := &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
		StartReplicationTaskType: aws.String("resume-processing"),
	}
	_, err := svc.StartReplicationTask(input)
	if err != nil {
		return err
	}
	return nil
}

// ReloadReplicationTask ...
//
// Todo
func ReloadReplicationTask(awsSession *session.Session, arn string) error {
	svc := databasemigrationservice.New(awsSession)
	input := &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn),
		StartReplicationTaskType: aws.String("reload-target"),
	}
	_, err := svc.StartReplicationTask(input)
	if err != nil {
		return err
	}
	return nil
}

// CheckReplicationTaskStatus ...
//
// Todo
func CheckReplicationTaskStatus(awsSession *session.Session, arn string) error {
	svc := databasemigrationservice.New(session.New())
	input := &databasemigrationservice.DescribeReplicationTasksInput{}
	results, err := svc.DescribeReplicationTasks(input)
	if err != nil {
		return err
	}
	for _, replicationTask := range results.ReplicationTasks {
		if *replicationTask.ReplicationTaskArn == arn {
			if *replicationTask.Status == "stopped" {
				return nil
			}
			return errors.New("Task not stopped: status: " + *replicationTask.Status)
		}
	}
	return errors.New("Task not found: " + arn)
}
