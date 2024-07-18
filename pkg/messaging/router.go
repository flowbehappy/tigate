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
	handlers map[topicType]MessageHandler
}

func newRouter() *router {
	return &router{
		handlers: make(map[topicType]MessageHandler),
	}
}

func (r *router) registerHandler(msgType topicType, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgType] = handler
}

func (r *router) deRegisterHandler(topic topicType) {
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
				log.Info("router: close, since context done")
				return
			case msg := <-out:
				r.mu.RLock()
				handler, ok := r.handlers[msg.Topic]
				r.mu.RUnlock()
				if !ok {
					// todo: is this possible to happens ?
					log.Debug("no handler for message", zap.Any("msg", msg))
					continue
				}
				err := handler(msg)
				if err != nil {
					log.Error("router: close, since handle message failed", zap.Error(err), zap.Any("msg", msg))
					return
				}
			}
		}
	}()
}
