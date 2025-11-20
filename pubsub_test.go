package pubsub_test

import (
	"context"
	"fmt"
	"pubsub"
	"testing"
	"testing/synctest"
	"time"
)

func TestPublisher_Publish(t *testing.T) {
	t.Parallel()

	publisher := pubsub.NewPubSub(t.Context())

	for i := 0; i < 100; i++ {
		sub := publisher.Subscribe("test")

		go func() {
			for {
				select {
				case <-t.Context().Done():
					return
				case myMsg := <-sub:
					fmt.Println("received msg : ", string(myMsg))
				}
			}
		}()
	}

	for i := 0; i < 10000000; i++ {
		go publisher.Publish("test", []byte("Hello world"))
	}

	time.Sleep(time.Second * 60 * 5)
}

func TestSubscriber_Topics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := pubsub.NewPubSub(t.Context())

		firstSub := publisher.Subscribe("test")
		secondSub := publisher.Subscribe("other")
		firstMessageWanted := []byte("Test message")
		secondMessageWanted := []byte("Other message")

		var i int
		go func() {
			for {
				select {
				case <-t.Context().Done():
					return
				case msg := <-firstSub:
					if string(msg) != string(firstMessageWanted) {
						t.Errorf("received wrong message : %s, wanted : %s", string(msg), string(firstMessageWanted))
					} else {
						i++
					}
				case msg := <-secondSub:
					if string(msg) != string(secondMessageWanted) {
						t.Errorf("received wrong message : %s, wanted : %s", string(msg), string(secondMessageWanted))
					} else {
						i++
					}
				}
			}
		}()

		publisher.Publish("test", firstMessageWanted)
		publisher.Publish("other", secondMessageWanted)

		synctest.Wait()
		if i != 2 {
			t.Errorf("received wrong number of messages : %d, wanted : 2", i)
		}
	})
}

// This test is just checking that the pubsub is not blocked or slowed when there's a lot of subscribers with backpressure.
func TestBlockedConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testCtx, cancel := context.WithCancel(t.Context())

		publisher := pubsub.NewPubSub(testCtx)

		listOfBlockedSubscriberTopics := []string{"first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eight", "ninth", "tenth", "eleventh"}

		listOfBlockedSubscriber := make([]<-chan []byte, 11)
		for _, topic := range listOfBlockedSubscriberTopics {
			// Basically we subscribe and never listen on the channel which will cause it to be blocked
			listOfBlockedSubscriber = append(listOfBlockedSubscriber, publisher.Subscribe(topic))
		}

		normalConsumerSubscriber := publisher.Subscribe("clean-topic-that-will-be-consumed")

		for i := 0; i < 50; i++ {
			for _, blockedSubscriber := range listOfBlockedSubscriberTopics {
				publisher.Publish(blockedSubscriber, []byte(fmt.Sprintf("Message %d", i)))
			}
		}

		// Wait all the message has been sent to the slow consumers and are blocking.
		synctest.Wait()

		publisher.Publish("clean-topic-that-will-be-consumed", []byte("Last message"))

		timeout := time.After(time.Millisecond * 50)
		select {
		case <-normalConsumerSubscriber:
		case <-timeout:
			t.Fatalf("timeout reached without receiving message, " +
				"it seems that the slow subscribers are blocking the fast one")
		}

		cancel()
		synctest.Wait()
	})
}
