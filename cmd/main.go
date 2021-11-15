package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"math/rand"
	"strconv"
	"sync"
)

type request struct {
	CustomerId int `json:"customerId" validate:"required"`
	Id         int `json:"id" validate:"required"`
}

const (
	numberOfCustomers = 10
	numberOfRequests = 100
)

func main() {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String("eu-west-1")},
		Profile: "default",
	})

	if err != nil {
		log.Fatal(err)
	}

	s := sqs.New(sess)

	var queueUrl = "https://sqs.eu-west-1.amazonaws.com/236584826472/example.fifo"

	var wg sync.WaitGroup
	wg.Add(numberOfCustomers)

	for i := 0 ; i < numberOfCustomers ; i++{
		go startSend(i,&wg,s,&queueUrl)
	}

	wg.Wait()

}

func startSend(customerId int,wg *sync.WaitGroup,s *sqs.SQS,queueUrl *string){
	defer wg.Done()
	for i := 0 ; i < numberOfRequests ; i++{
		arr,_ := json.Marshal(request{
			CustomerId: customerId,
			Id:         i,
		})
		var body = string(arr)
		var messageGroupId = strconv.Itoa(customerId)
		var messageDeduplicationId = fmt.Sprintf("%s:%s",strconv.Itoa(customerId),strconv.Itoa(i))
		m := sqs.SendMessageInput{
			MessageBody: &body,
			MessageGroupId: &messageGroupId,
			MessageDeduplicationId: &messageDeduplicationId,
			QueueUrl: queueUrl,
		}

		// publish randomly more than once - simulate network partition
		for i := 0; i == 0 || rand.Intn(2) == 0 ; i++ {
			_,err := s.SendMessage(&m)
			if err != nil {
				log.Printf("err : %s",err)
			}
		}

	}
}
