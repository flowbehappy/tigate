package messaging

import (
	"context"
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, msg *TargetMessage) error

type router struct {
	mu       sync.RWMutex
	handlers map[common.TopicType]MessageHandler
}

func newRouter() *router {
	return &router{
		handlers: make(map[common.TopicType]MessageHandler),
	}
}

func (r *router) registerHandler(msgType common.TopicType, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[msgType] = handler
}

func (r *router) deRegisterHandler(topic common.TopicType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, topic)
}

func (r *router) runDispatch(ctx context.Context, wg *sync.WaitGroup, out <-chan *TargetMessage) {
	wg.Add(1)
	batchSize := 32
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("router: close, since context done")
				return
			case msg := <-out:
				batchMsg := make([]*TargetMessage, 0, 32)
				batchMsg = append(batchMsg, msg)
			loop:
				for i := 0; i < batchSize; i++ {
					select {
					case msg := <-out:
						batchMsg = append(batchMsg, msg)
					default:
						break loop
					}
				}
				for _, msg := range batchMsg {
					r.mu.RLock()
					handler, ok := r.handlers[msg.Topic]
					r.mu.RUnlock()
					if !ok {
						// todo: is this possible to happens ?
						log.Debug("no handler for message", zap.Any("msg", msg))
						continue
					}
					err := handler(ctx, msg)
					if err != nil {
						log.Error("router: close, since handle message failed", zap.Error(err), zap.Any("msg", msg))
						return
					}
				}
			}
		}
	}()
}
