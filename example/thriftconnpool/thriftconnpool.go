package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/joychenjh/connpool"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/joychenjh/connpool/example/thriftconnpool/gen-go/example"
)

var addr = flag.String("addr", "127.0.0.1:9090", "srv addr")

type TestSrv struct {
	srv  *thrift.TSimpleServer
	Addr string
	num  int32
}

var Srv *TestSrv

func (s *TestSrv) Ping(meta string) (r string, err error) {
	return meta, err
}

func (s *TestSrv) Echo(meta string) (r string, err error) {

	//模拟应用中的异常.
	if atomic.AddInt32(&s.num, 1)%2 == 0 {
		return r, fmt.Errorf("echo error")
	}
	return meta, err
}

func (s *TestSrv) Stop() {
	if s.srv != nil {
		s.srv.Stop()
	}
}

func StartSrv(addr string) (srv *TestSrv, err error) {
	srv = &TestSrv{
		Addr: addr,
	}
	processor := example.NewConnPoolExampleProcessor(srv)

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return srv, err
	}

	srv.srv = thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	go srv.srv.Serve()

	return srv, nil
}

type ExampleClient struct {
	Socket *thrift.TSocket
	Client *example.ConnPoolExampleClient
}

func connPro(wg *sync.WaitGroup, addr string) (err error) {
	defer wg.Done()

	pool, err := NewConnPool(addr)
	if err != nil {
		return err
	}
	cwg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		cwg.Add(1)
		go clientprocess(cwg, pool, i)
	}
	cwg.Wait()
	return err
}

func GetClient(pool *connpool.ConnPool) (tran *connpool.Transport, client *example.ConnPoolExampleClient, err error) {
	tran, err = pool.Get(context.TODO())
	if err != nil {
		return tran, client, err
	}

	_c, ok := tran.TC().(*ExampleClient)
	if !ok {
		return tran, client, fmt.Errorf("pool tran type :%T err", tran.TC())
	}

	client = _c.Client

	return tran, client, err
}

func clientprocess(wg *sync.WaitGroup, pool *connpool.ConnPool, index int) (err error) {
	defer func() {
		wg.Done()
		log.Printf("client index :%v exit err:%v", index, err)
	}()

	for i := 0; i < 10; i++ {

		tran, client, err := GetClient(pool)
		if err != nil {
			log.Printf("GetClient err:", err)
		} else {

			if i == 5 {
				//模拟连接异常.
				client.Transport.Close()
			}
			req := fmt.Sprintf("index:%v_%v", index, i)
			res, err := client.Echo(req)
			tran.Close(CheckThriftConnErr(err))
			if err != nil {
				log.Printf("Echo err:%v", err)
			} else {
				log.Printf("Echo OK req:%s, res:%v", req, res)
			}
		}
	}

	return err
}

func CheckThriftConnErr(err error) bool {
	if err != nil {
		if _, ok := err.(thrift.TTransportException); ok {
			return true
		}
	}
	return false
}

func NewConnPool(addr string) (pool *connpool.ConnPool, err error) {
	dialFunc := func(meta interface{}) (connpool.TClient, error) {
		c := &ExampleClient{}

		var transport thrift.TTransport
		var err error
		c.Socket, err = thrift.NewTSocket(addr)
		if err != nil {
			return c, err
		}
		transport = thrift.NewTFramedTransport(c.Socket)
		if err = transport.Open(); err != nil {
			return transport, err
		}
		log.Printf("connet conn %v <-> %v", c.Socket.Conn().RemoteAddr(), c.Socket.Conn().LocalAddr())

		c.Client = example.NewConnPoolExampleClientFactory(transport, thrift.NewTBinaryProtocolFactoryDefault())
		return c, nil
	}

	closeFunc := func(c connpool.TClient, meta interface{}) (err error) {

		_c, ok := c.(*ExampleClient)
		if !ok {
			return fmt.Errorf("closefunc client type:%T", c)
		}
		if _c.Socket != nil && _c.Socket.IsOpen() {
			log.Printf("close  conn %v <-> %v", _c.Socket.Conn().RemoteAddr(), _c.Socket.Conn().LocalAddr())
			err = _c.Socket.Close()
		}
		return err
	}

	heartbeatFunc := func(c connpool.TClient, meta interface{}) (err error) {
		_c, ok := c.(*ExampleClient)
		if !ok {
			return fmt.Errorf("closefunc client type:%T", c)
		}
		res, err := _c.Client.Ping("ok")
		if err != nil {
			return err
		}
		if res != "ok" {
			return fmt.Errorf("Heartbeat err:%v", res)
		}
		return nil
	}

	pool = &connpool.ConnPool{
		Dial:      dialFunc,
		Heartbeat: heartbeatFunc,
		CloseConn: closeFunc,
		MaxActive: 5,
	}

	if err := pool.Init(); err != nil {
		return pool, err
	}
	return pool, err
}

func main() {

	flag.Parse()

	srv, err := StartSrv(*addr)
	if err != nil {
		log.Println("start server err:", err)
		return

	}

	Srv = srv

	wg := &sync.WaitGroup{}

	wg.Add(1)
	connPro(wg, *addr)

	wg.Wait()

	srv.Stop()
}
