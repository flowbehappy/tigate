package messaging

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, msg *TargetMessage) error

type router struct {
	mu       sync.RWMutex
	handlers map[string]MessageHandler
}

func newRouter() *router {
	return &router{
		handlers: make(map[string]MessageHandler),
	}
}

func (r *router) registerHandler(topic string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic] = handler
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
				log.Info("router: close, since context done")
				return
			case msg := <-out:
				r.mu.RLock()
				handler, ok := r.handlers[msg.Topic]
				r.mu.RUnlock()
				if !ok {
					log.Debug("no handler for message, drop it", zap.Any("msg", msg))
					continue
				}
				err := handler(ctx, msg)
				if err != nil {
					log.Error("router: close, since handle message failed", zap.Error(err), zap.Any("msg", msg))
				}
			}
		}
	}()
}
