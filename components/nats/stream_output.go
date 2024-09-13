package nats

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redpanda-data/benthos/v4/public/service"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
)

const (
	natsFieldMessageDedupeID = "msg_id"
	natsFieldAckWait         = "ack_wait"
	natsFieldBatching        = "batching"
)

func natsJetStreamOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("3.46.0").
		Summary("Write messages to a NATS JetStream subject.").
		Description(connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewInterpolatedStringField("subject").
			Description("A subject to write to.").
			Example("foo.bar.baz").
			Example(`${! meta("kafka_topic") }`).
			Example(`foo.${! json("meta.type") }`)).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Content-Type": "application/json",
				"Timestamp":    `${!meta("Timestamp")}`,
			}).Version("4.1.0")).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewOutputMaxInFlightField().Default(1024)).
		Fields(
			service.NewInterpolatedStringField(natsFieldMessageDedupeID).
				Description("An optional deduplication ID to set for messages.").
				Optional(),
		).
		Field(service.NewDurationField(natsFieldAckWait).
			Description("Maximum time to wait for receiving publish acknowledgements.").
			Default("2s").
			Optional()).
		Fields(service.NewBatchPolicyField(natsFieldBatching)).
		Fields(connectionTailFields()...).
		Fields(outputTracingDocs())
}

func init() {
	err := service.RegisterBatchOutput(
		"jetstream_stream", natsJetStreamOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(natsFieldBatching); err != nil {
				return
			}
			var w *jetStreamOutput
			if w, err = newJetStreamWriterFromConfig(conf, mgr); err != nil {
				return
			}
			out, err = conf.WrapBatchOutputExtractTracingSpanMapping("jetstream_stream", w)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jetStreamOutput struct {
	connDetails            connectionDetails
	subjectStrRaw          string
	subjectStr             *service.InterpolatedString
	headers                map[string]*service.InterpolatedString
	metaFilter             *service.MetadataFilter
	messageDeduplicationID *service.InterpolatedString
	ackWait                time.Duration

	log *service.Logger

	connMut  sync.Mutex
	natsConn *nats.Conn
	js       jetstream.JetStream

	shutSig *shutdown.Signaller

	// The pool caller id. This is a unique identifier we will provide when calling methods on the pool. This is used by
	// the pool to do reference counting and ensure that connections are only closed when they are no longer in use.
	pcid string
}

func newJetStreamWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*jetStreamOutput, error) {
	j := jetStreamOutput{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
		pcid:    uuid.New().String(),
	}

	var err error
	if j.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if j.subjectStrRaw, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if j.subjectStr, err = conf.FieldInterpolatedString("subject"); err != nil {
		return nil, err
	}

	if j.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}

	if conf.Contains("metadata") {
		if j.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}

	if conf.Contains(natsFieldMessageDedupeID) {
		if j.messageDeduplicationID, err = conf.FieldInterpolatedString(natsFieldMessageDedupeID); err != nil {
			return nil, err
		}
	}

	if j.ackWait, err = conf.FieldDuration(natsFieldAckWait); err != nil {
		return nil, err
	}

	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) Connect(ctx context.Context) (err error) {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var js jetstream.JetStream

	defer func() {
		if err != nil && natsConn != nil {
			_ = pool.Release(j.pcid, j.connDetails)
		}
	}()

	if natsConn, err = pool.Get(ctx, j.pcid, j.connDetails); err != nil {
		return err
	}

	if js, err = jetstream.New(natsConn); err != nil {
		return err
	}

	j.natsConn = natsConn
	j.js = js
	return nil
}

func (j *jetStreamOutput) disconnect() {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		_ = pool.Release(j.pcid, j.connDetails)
		j.natsConn = nil
	}
	j.js = nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	j.connMut.Lock()
	js := j.js
	j.connMut.Unlock()
	if js == nil {
		return service.ErrNotConnected
	}

	for i, msg := range batch {
		subject, err := j.subjectStr.TryString(msg)
		if err != nil {
			return fmt.Errorf(`failed string interpolation on field "subject": %w`, err)
		}

		var dedupeID string
		if j.messageDeduplicationID != nil {
			dedupeID, err = batch.TryInterpolatedString(i, j.messageDeduplicationID)
			if err != nil {
				return fmt.Errorf("dedupe id interpolation: %w", err)
			}
		}

		jsmsg := nats.NewMsg(subject)
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		jsmsg.Data = msgBytes
		for k, v := range j.headers {
			value, err := v.TryString(msg)
			if err != nil {
				return fmt.Errorf(`failed string interpolation on header %q: %w`, k, err)
			}

			jsmsg.Header.Add(k, value)
		}
		_ = j.metaFilter.Walk(msg, func(key, value string) error {
			jsmsg.Header.Add(key, value)
			return nil
		})

		_, err = js.PublishMsgAsync(jsmsg, jetstream.WithMsgID(dedupeID))
		if err != nil {
			return err
		}
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(j.ackWait):
		return fmt.Errorf("publish took too long")
	}
	return nil
}

func (j *jetStreamOutput) Close(ctx context.Context) error {
	go func() {
		j.disconnect()
		j.shutSig.TriggerHasStopped()
	}()
	select {
	case <-j.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
