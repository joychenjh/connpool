

# Connection Pool Client

   通用连接池,可以用于Socket pool/Thrift Client pool/Socket连接池,Thrift连接池等场景.
   
  
# 特点

  1.支持自定义新建连接,心跳,连接关闭.
  
  2.支持连接最大连接数限制.


# 用法

     go get github.com/joychenjh/connpool



## 建立Thirft Client连接池示例

   定义连接池,定义新建连接,心跳,连接关闭操作.

```go
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
	
```

	
连接池业务代码:

```go

    //用于判断是不是传输层的错误, 如果是传输层的错误, 应该关闭连接.
	func CheckThriftConnErr(err error) bool {
    	if err != nil {
    		if _, ok := err.(thrift.TTransportException); ok {
    			return true
    		}
    	}
    	return false
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

```

使用连接池.

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
