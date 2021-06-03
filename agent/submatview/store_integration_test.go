package submatview_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/agent/rpcclient/health"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/rpc/subscribe"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/submatview"
	"github.com/hashicorp/consul/proto/pbsubscribe"
)

func TestStore_IntegrationWithBackend(t *testing.T) {
	handlers := map[stream.Topic]stream.SnapshotFunc{
		pbsubscribe.Topic_ServiceHealth: func(stream.SubscribeRequest, stream.SnapshotAppender) (index uint64, err error) {
			// TODO: add a couple services?
			return 1, nil
		},
	}
	pub := stream.NewEventPublisher(handlers, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	addr := newServer(t, pub)

	// TODO: make configurable
	runTime := 3 * time.Second
	ctx, cancel = context.WithTimeout(ctx, runTime)
	defer cancel()

	store := submatview.NewStore(hclog.New(nil))
	go store.Run(ctx)

	producers := []*eventProducer{
		newEventProducer(pub, pbsubscribe.Topic_ServiceHealth, "srv1"),
		// TODO:
		//newEventProducer(pub, pbsubscribe.Topic_ServiceHealth, "srv2"),
	}

	pgroup, pctx := errgroup.WithContext(ctx)
	for i := range producers {
		producer := producers[i]
		pgroup.Go(func() error {
			producer.Produce(pctx)
			return nil
		})
	}

	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)

	c := health.Client{
		UseStreamingBackend: true,
		ViewStore:           store,
		MaterializerDeps: health.MaterializerDeps{
			Conn:   conn,
			Logger: hclog.New(nil),
		},
	}

	req := structs.ServiceSpecificRequest{
		ServiceName: "srv1",
	}
	updateCh := make(chan cache.UpdateEvent, 10)
	states := map[uint64][]string{}

	cgroup, cctx := errgroup.WithContext(ctx)
	cgroup.Go(func() error {
		return c.Notify(cctx, req, "", updateCh)
	})
	cgroup.Go(func() error {
		var idx uint64
		for {
			last := producers[0].LastIndex()
			if last > 0 && idx == last {
				return nil
			}
			select {
			case u := <-updateCh:
				idx = u.Meta.Index
				states[u.Meta.Index] = stateFromUpdates(u)
			case <-ctx.Done():
				return nil
			}
		}
	})

	_ = pgroup.Wait()
	_ = cgroup.Wait()

	require.True(t, len(states) > 10, "expected more than %d events", len(states))
	for idx, nodes := range states {
		require.Equal(t, producers[0].nodesByIndex[idx], nodes, idx)
	}
}

func stateFromUpdates(u cache.UpdateEvent) []string {
	var result []string
	for _, node := range u.Result.(*structs.IndexedCheckServiceNodes).Nodes {
		result = append(result, node.Node.Node)
	}

	sort.Strings(result)
	return result
}

func newServer(t *testing.T, pub *stream.EventPublisher) net.Addr {
	subSrv := &subscribe.Server{
		Backend: backend{pub: pub},
		Logger:  hclog.New(nil),
	}
	srv := grpc.NewServer()
	pbsubscribe.RegisterStateChangeSubscriptionServer(srv, subSrv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var g errgroup.Group
	g.Go(func() error {
		return srv.Serve(lis)
	})
	t.Cleanup(func() {
		srv.Stop()
		if err := g.Wait(); err != nil {
			t.Log(err.Error())
		}
	})

	return lis.Addr()
}

type backend struct {
	pub *stream.EventPublisher
}

func (b backend) ResolveTokenAndDefaultMeta(string, *structs.EnterpriseMeta, *acl.AuthorizerContext) (acl.Authorizer, error) {
	return acl.AllowAll(), nil
}

func (b backend) Forward(string, func(*grpc.ClientConn) error) (handled bool, err error) {
	return false, nil
}

func (b backend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	return b.pub.Subscribe(req)
}

var _ subscribe.Backend = (*backend)(nil)

type eventProducer struct {
	rand         *rand.Rand
	pub          *stream.EventPublisher
	topic        stream.Topic
	srvName      string
	nodesByIndex map[uint64][]string
	done         chan struct{}
	lastIndex    uint64
}

func newEventProducer(
	pub *stream.EventPublisher,
	topic stream.Topic,
	srvName string,
) *eventProducer {
	return &eventProducer{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		nodesByIndex: map[uint64][]string{},
		done:         make(chan struct{}),
		pub:          pub,
		topic:        topic,
		srvName:      srvName,
	}
}

var minEventDelay = 10 * time.Millisecond

func (e *eventProducer) Produce(ctx context.Context) {
	var nodes []string
	var nextID int
	var idx uint64 = 3

	defer func() {
		e.lastIndex = idx
		close(e.done)
	}()

	for ctx.Err() == nil {
		var event stream.Event

		action := e.rand.Intn(3)
		if len(nodes) == 0 {
			action = 1
		}

		switch action {

		case 0: // Deregister
			nodeIdx := e.rand.Intn(len(nodes))
			node := nodes[nodeIdx]
			nodes = append(nodes[:nodeIdx], nodes[nodeIdx+1:]...)

			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Deregister,
					Value: &structs.CheckServiceNode{
						Node:    &structs.Node{Node: node},
						Service: &structs.NodeService{ID: e.srvName},
					},
				},
			}
			fmt.Printf("%d: DEREG %v\n", idx, node)

		case 1: // Register new
			node := nodeName(nextID)
			nodes = append(nodes, node)
			nextID++

			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Register,
					Value: &structs.CheckServiceNode{
						Node: &structs.Node{Node: node},
						Service: &structs.NodeService{
							ID:      e.srvName,
							Service: e.srvName,
						},
					},
				},
			}
			fmt.Printf("%d: REG   %v\n", idx, node)

		case 2: // Register update
			node := nodes[e.rand.Intn(len(nodes))]
			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Register,
					Value: &structs.CheckServiceNode{
						Node: &structs.Node{Node: node},
						Service: &structs.NodeService{
							ID:      e.srvName,
							Service: e.srvName,
						},
					},
				},
			}

			fmt.Printf("%d: UPD   %v\n", idx, node)
		}

		e.pub.Publish([]stream.Event{event})
		e.nodesByIndex[idx] = copyNodeList(nodes)

		idx++
		delay := time.Duration(rand.Intn(50)) * time.Millisecond
		time.Sleep(minEventDelay + delay)
	}
}

func (e *eventProducer) LastIndex() uint64 {
	select {
	case <-e.done:
		return e.lastIndex
	default:
		return 0
	}
}

func nodeName(i int) string {
	return fmt.Sprintf("node-%d", i)
}

func copyNodeList(nodes []string) []string {
	result := make([]string, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	sort.Strings(result)
	return result
}
