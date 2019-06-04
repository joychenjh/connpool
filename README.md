

# Connection Pool Client

   通用连接池,可以用于Socket pool/Thrift Client pool/Socket连接池,Thrift连接池等场景.


# 功能

  1.支持自定义新建连接操作,心跳操作,连接关闭操作.
  2.支持连接最大连接数限制.

# 用法

     go get github.com/joychenjh/connpool


# ConnPool说明:

```go
    ConnPool

	//连接建立函数.
	Dial func(meta interface{}) (TClient, error)

	//心跳/连接检查函数.
	//检查时机:a.Dail调用成功后. b.函数在HeartbeatInterval秒以前使用,再次使用时. c.保障MaxIdle个连接可用.
	Heartbeat func(c TClient, meta interface{}) error

	//连接关闭处理.
	CloseConn func(c TClient, meta interface{}) error

	// 调用Dial连接失败时重试次数. 默认为1.
	DialRetryCount int

	// 空闲连接释放时间时间 2*60s
	IdleTimeout time.Duration

	//心跳检测时间 默认60s
	HeartbeatInterval time.Duration

	// 最大活跃连接数.
	MaxIdle int

	// 最大连接数.
	MaxActive int

	//当为true时, 超过最大连接数后会等带别的连接释放.
	//当为false时, 超过最大连接数后还是会建立新的连接.
	Wait bool
```


# 使用示例.

## 建立Thirft Client连接池示例
  请参考[Thriftconnpool](https://github.com/joychenjh/connpool/tree/master/example/thriftconnpool).


### 自定义连接结构:

```go
type ExampleClient struct {
	Socket *thrift.TSocket
	Client *example.ConnPoolExampleClient
}

```

### 从连接池中获取连接.

```go
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

    //用于判断是不是传输层的错误, 如果是传输层的错误, 应该关闭连接.
   	func CheckThriftConnErr(err error) bool {
       	if err != nil {
       			return true
       	}
       	return false
    }
```

### 初始化连接池

   定义连接池,定义新建连接,心跳,连接关闭操作.

```go
    //连接操作.
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

    //关闭操作
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

    //心跳操作.
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
	
```

	
### 连接池使用.
```go
    		tran, client, err := GetClient(pool)
    		if err != nil {
    			log.Printf("GetClient err:", err)
    		} else {

    			req := fmt.Sprintf("index:%v_%v", index, i)
    			res, err := client.Echo(req)
    			tran.Close(CheckThriftConnErr(err))
			if err != nil {
			....
			}
                ...
    		}

```


# License

The MIT License (MIT) - see LICENSE for more details
