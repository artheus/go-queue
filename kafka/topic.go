package kafka

import (
	"github.com/segmentio/kafka-go"
	"gopkg.in/src-d/go-queue.v1"
	"time"
)

type Topic struct {
	ctx       *topicCtx
	r         *kafka.Reader
	w         *kafka.Writer
	name      string
}

func (t *Topic) Cancel() {
	t.ctx.Cancel()
}

func (t *Topic) jobToMessage(job *queue.Job) kafka.Message {
	return kafka.Message{
		Topic: t.name,
		Key:   []byte(job.ID),
		Value: job.Raw,
		Time:  job.Timestamp,
	}
}

func (t *Topic) Publish(job *queue.Job) error {
	return t.w.WriteMessages(t.ctx,t.jobToMessage(job))
}

func (t *Topic) PublishDelayed(job *queue.Job, delay time.Duration) (err error) {
	time.Sleep(delay)

	return t.w.WriteMessages(t.ctx,t.jobToMessage(job))
}

func (t *Topic) Transaction(callback queue.TxCallback) (err error) {
	// TODO: Implement
	return nil
}

func (t *Topic) Consume(advertisedWindow int) (queue.JobIter, error) {
	// TODO: Implement
	return nil, nil
}

func (t *Topic) RepublishBuried(conditions ...queue.RepublishConditionFunc) error {
	// TODO: Implement
	return nil
}
