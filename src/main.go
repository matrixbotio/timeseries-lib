package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws/credentials"
)

func createSession() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials("AKID", "SECRET_KEY", "TOKEN"),
	})
	if err != nil {
		panic("Cannot create AWS session. Aborting...")
	}
	return sess
}

func main() {
	sess := createSession()
	//
}
