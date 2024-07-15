package messaging

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type MessageHandler func(msg *TargetMessage) error

type router struct {
	mu       sync.RWMutex
	handlers map[string]MessageHandler
}

func newRouter() *router {
	return &router{
		handlers: make(map[string]MessageHandler),
	}
}

func (r *router) registerHandler(msgType string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgType] = handler
}

func (r *router) deRegisterHandler(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, topic)
}

func (r *router) runDispatch(ctx context.Context, wg *sync.WaitGroup, out <-chan *TargetMessage) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("message router is closing")
			case msg := <-out:
				r.mu.RLock()
				handler, ok := r.handlers[msg.Topic]
				r.mu.RUnlock()
				if !ok {
					log.Debug("no handler for message", zap.Any("msg", msg))
					continue
				}
				_ = handler(msg)
			}
		}
	}()
}
