// Pubsub implements a PubSub library, this is an internal package and for now does not
// expose online broker, so it can only be used within the same process.

// Rules of business, be aware of them before using this package:
//   - The order is not guaranteed.
//   - Some message may be lost if your consumer are too slow, I made this decision because
//     I'm building this pkg for sse, the example I had in mind was datadog, send new metrics every
//     x times, so it make sense to just drop data since they are not primordially important.
//     the correct refresh of the graphs are good but not critical.
package pubsub

import (
	"context"
	"sync"
)

func NewPubSub(ctx context.Context) *Publisher {
	p := &Publisher{
		// channels is a map that store a list of subscribers (chan of bytes) by topic.
		channels:       make(map[string][]chan []byte),
		balancer:       make(chan *evt, 1000),
		subscribeMutex: &sync.Mutex{},
	}

	// This is the load balancer, it will get all the message and redistribute them so that the Publish
	// won't block too much.
	go func() {
		for {
			select {
			// Graceful shutdown
			case <-ctx.Done():
				p.close()
				p.channels = nil
				p.balancer = nil
				return
			case curEvt := <-p.balancer:
				p.sendMsg(curEvt.topic, curEvt.msg)
			}
		}

	}()

	return p
}

type evt struct {
	topic string
	msg   []byte
}

type Publisher struct {
	// channels represent a list of subscribers for a given topic
	channels       map[string][]chan []byte
	balancer       chan *evt
	subscribeMutex *sync.Mutex
}

// sendMsg implements a non-blocking fan-out that we always try to send to the channel, but we are not
// waiting for it to receive the message, if the channel is full, we just drop the message and never retry.
func (p *Publisher) sendMsg(topic string, msg []byte) {
	for _, subscriber := range p.channels[topic] {
		select {
		case subscriber <- msg:
			return
		default:
			return
		}
	}
}

func (p *Publisher) close() {
	for _, subscribers := range p.channels {
		for _, subscriber := range subscribers {
			close(subscriber)
		}
	}

	close(p.balancer)
}

func (p *Publisher) Publish(topic string, msg []byte) {
	p.balancer <- &evt{topic, msg}
}

func (p *Publisher) Subscribe(topic string) <-chan []byte {
	p.subscribeMutex.Lock()
	defer p.subscribeMutex.Unlock()

	newSubscriber := make(chan []byte, 10)
	p.channels[topic] = append(p.channels[topic], newSubscriber)
	return newSubscriber
}
