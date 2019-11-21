package main

import (
	"encoding/json"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
)

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

func newAuth(aclData string) (*Auth, error) {
	auth := Auth{}
	err := json.Unmarshal([]byte(aclData), &auth.AclData)
	if err != nil {
		return nil, errors.New("expacted error on bad acl json, have nil")
	}
	return &auth, nil
}

type Auth struct {
	AclData map[string][]string
}

func (a *Auth) checkAuth(ctx context.Context, method string) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md.Get("consumer")
	if len(consumer) != 1 {
		return false

	}
	rules, ok := a.AclData[consumer[0]]
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

func (a *Auth) authInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if !a.checkAuth(ctx, info.FullMethod) {
		return nil, status.Error(codes.Unauthenticated, "")
	}
	return handler(ctx, req)
}

func (a *Auth) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if !a.checkAuth(ss.Context(), info.FullMethod) {
		return status.Error(codes.Unauthenticated, "")
	}
	return handler(srv, ss)
}

func gRPCService(addr string, aclData string) (func(), error) {
	auth, err := newAuth(aclData)
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln("failed to listen TCP port", err)
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(auth.authInterceptor),
		grpc.StreamInterceptor(auth.streamInterceptor),
	)

	RegisterAdminServer(server, new(UnimplementedAdminServer))
	RegisterBizServer(server, new(MyBizServer))

	go server.Serve(lis)
	return func() {
		server.GracefulStop()
	}, nil
}
