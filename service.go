package main

import (
	"encoding/json"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

func (s *Stat) incrementByMethod(method string) {
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	_, ok := s.ByMethod[method]
	if !ok {
		s.ByMethod[method] = 1
	} else {
		s.ByMethod[method]++
	}
}
func (s *Stat) incrementByConsumer(consumer string) {
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	_, ok := s.ByConsumer[consumer]
	if !ok {
		s.ByConsumer[consumer] = 1
	} else {
		s.ByConsumer[consumer]++
	}
}

type MyBizServer struct {
}

func (*MyBizServer) Check(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
func (*MyBizServer) Add(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
func (*MyBizServer) Test(ctx context.Context, req *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

type MyAdminServer struct {
	stat        *Stat
	LogChan     chan Event
	logClients  []chan Event
	statClients []chan StatMessage
	mu          *sync.RWMutex
}

func (as *MyAdminServer) pushLogsAndStatsToClients() {
	for event := range as.LogChan {
		as.stat.incrementByConsumer(event.Consumer)
		as.stat.incrementByMethod(event.Method)
		as.mu.Lock()
		logClients := as.logClients
		statClients := as.statClients
		as.mu.Unlock()
		for _, logClient := range logClients {
			logClient <- event
		}
		for _, statClient := range statClients {
			statClient <- StatMessage{event.Method, event.Consumer}
		}
	}
}

func (as *MyAdminServer) Logging(req *Nothing, srv Admin_LoggingServer) error {
	clientChan := make(chan Event)
	as.mu.Lock()
	as.logClients = append(as.logClients, clientChan)
	as.mu.Unlock()
	for event := range clientChan {
		srv.Send(&event)
	}
	return nil
}

type StatMessage struct {
	method   string
	consumer string
}

func (as *MyAdminServer) Statistics(req *StatInterval, srv Admin_StatisticsServer) error {
	statChan := make(chan StatMessage)
	as.mu.Lock()
	as.statClients = append(as.statClients, statChan)
	as.mu.Unlock()
	ticker := time.NewTicker(time.Second * time.Duration(req.IntervalSeconds))
	defer ticker.Stop()
	stat := &Stat{ByMethod: map[string]uint64{}, ByConsumer: map[string]uint64{}}
	s := StatMessage{}
	for {
		select {
		case s = <-statChan:
			stat.incrementByMethod(s.method)
			stat.incrementByConsumer(s.consumer)
		case <-ticker.C:
			as.mu.Lock()
			srv.Send(stat)
			stat = &Stat{ByMethod: map[string]uint64{}, ByConsumer: map[string]uint64{}}
			as.mu.Unlock()
		}
	}
}

func StartMyMicroservice(ctx context.Context, addr string, data string) error {
	stopService, err := gRPCService(addr, data)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				stopService()
				return
			default:
				continue
			}
		}
	}()
	return nil
}

func gRPCService(addr string, aclData string) (func(), error) {
	m, err := newMiddleware(aclData)
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln("failed to listen TCP port", err)
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(m.unaryInterceptor),
		grpc.StreamInterceptor(m.streamInterceptor),
	)
	adminSrv := new(MyAdminServer)
	adminSrv.LogChan = m.Log

	stat := new(Stat)
	stat.ByConsumer = make(map[string]uint64)
	stat.ByMethod = make(map[string]uint64)

	adminSrv.stat = stat
	adminSrv.mu = &sync.RWMutex{}
	RegisterAdminServer(server, adminSrv)
	RegisterBizServer(server, new(MyBizServer))

	go adminSrv.pushLogsAndStatsToClients()

	go server.Serve(lis)
	return func() {
		close(m.Log)
		for _, client := range adminSrv.logClients {
			close(client)
		}
		server.GracefulStop()
	}, nil
}

func newMiddleware(aclData string) (*Middleware, error) {
	m := Middleware{}
	m.Log = make(chan Event, 5)
	err := json.Unmarshal([]byte(aclData), &m.AclData)
	if err != nil {
		return nil, errors.New("expacted error on bad acl json, have nil")
	}
	return &m, nil
}

type Middleware struct {
	Log     chan Event
	AclData map[string][]string
}

func (m *Middleware) checkAuth(ctx context.Context, method string) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md.Get("consumer")
	if len(consumer) != 1 {
		return false

	}
	rules, ok := m.AclData[consumer[0]]
	if !ok {
		return false
	}
	parsedMethod := strings.Split(method, "/")
	for _, rule := range rules {
		parsedRule := strings.Split(rule, "/")
		if parsedMethod[1] == parsedRule[1] {
			if parsedRule[2] == "*" || parsedMethod[2] == parsedRule[2] {
				return true
			}
		}
	}
	return false
}

func (m *Middleware) pushEventToLog(ctx context.Context, methodName string) {

	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md.Get("consumer")
	p, _ := peer.FromContext(ctx)
	event := Event{}
	event.Consumer = consumer[0]
	event.Method = methodName
	event.Host = p.Addr.String()
	m.Log <- event
}

func (m *Middleware) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if !m.checkAuth(ctx, info.FullMethod) {
		return nil, status.Error(codes.Unauthenticated, "")
	}
	m.pushEventToLog(ctx, info.FullMethod)

	return handler(ctx, req)
}

func (m *Middleware) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if !m.checkAuth(ss.Context(), info.FullMethod) {
		return status.Error(codes.Unauthenticated, "")
	}
	m.pushEventToLog(ss.Context(), info.FullMethod)
	return handler(srv, ss)
}
