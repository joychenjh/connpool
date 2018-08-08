package connpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var index int32

type SocketConn struct {
	info string
	num  int32
}

func (s *SocketConn) String() string {

	return fmt.Sprintf("SocketConn num:%d info:%s", s.num, s.info)
}
func GetDefPool(t *testing.T) *ConnPool {
	pool := &ConnPool{
		Dial: func(meta interface{}) (TClient, error) {
			n, _ := meta.(*int32)
			return &SocketConn{info: "test Dial", num: atomic.AddInt32(n, 1)}, nil
		},
		CloseConn: func(c TClient, meta interface{}) (err error) {
			t.Logf("CloseConn index:%d", c.(*SocketConn).num)
			return nil
		},
		UserMeta: &index,
	}
	return pool
}

func GetConn(ctx context.Context, pool *ConnPool) (_c *Transport, conn *SocketConn, err error) {
	_c, err = pool.Get(ctx)
	if err != nil {
		return _c, conn, err
	}

	var ok bool
	conn, ok = _c.TC().(*SocketConn)
	if !ok {
		return _c, conn, fmt.Errorf("pool Get Transport type :%T", _c.TC())
	}
	return _c, conn, err
}

func TestConnPool_Init(t *testing.T) {
	pool := &ConnPool{}
	if err := pool.Init(); err != ErrDialNil {
		t.Fatal("Init Err:", err)
	}
}

func TestConnPool_DialErr(t *testing.T) {

	dialErr := errors.New("Dial error")
	pool := GetDefPool(t)
	pool.Dial = func(meta interface{}) (TClient, error) {
		return nil, dialErr
	}

	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		_, err := pool.Get(context.TODO())
		if err != dialErr {
			t.Error("pool get need err:", err)
		}
	}
}
func TestConnPool_PingErr(t *testing.T) {

	dialErr := errors.New("Heartbeat error")
	pool := GetDefPool(t)

	pool.Heartbeat = func(c TClient, meta interface{}) (err error) {
		return dialErr
	}

	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		_, err := pool.Get(context.TODO())
		if err != dialErr {
			t.Error("pool get need err:", err)
		}
	}
}

func TestConnPool_Heartbeat(t *testing.T) {

	pool := GetDefPool(t)

	pool.Heartbeat = func(c TClient, meta interface{}) (err error) {
		t.Log("Heartbeat ")
		return nil
	}
	pool.HeartbeatInterval = 5 * time.Microsecond

	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Microsecond)
		pool.Get(context.TODO())
	}
}

func TestConnPool_GetWait(t *testing.T) {

	pool := GetDefPool(t)
	pool.Wait = true
	pool.MaxActive = 3

	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < pool.MaxActive; i++ {
		_, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())
	}

	//
	ctx, _ := context.WithTimeout(context.TODO(), time.Second)
	_, _, err := GetConn(ctx, pool)
	if err != nil {
		t.Log("pool get err:", err, "ActiveConn:", pool.AcivteConn())
	} else {
		t.Error("pool get err:", err, "ActiveConn:", pool.AcivteConn())
	}
}

func TestConnPool_GetWait1(t *testing.T) {

	pool := GetDefPool(t)
	pool.Wait = true
	pool.MaxActive = 3

	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	tranArr := []*Transport{}
	for i := 0; i < pool.MaxActive; i++ {
		_tran, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())
		tranArr = append(tranArr, _tran)
	}

	go func() {
		for _, _v := range tranArr {
			time.Sleep(time.Second)
			_v.Close(false)
		}
	}()

	for i := 0; i < pool.MaxActive; i++ {
		ctx, _ := context.WithTimeout(context.TODO(), 2*time.Second)
		_, _c, err := GetConn(ctx, pool)
		if err != nil {
			t.Log("pool get err:", err, "ActiveConn:", pool.AcivteConn())
		} else {
			t.Error("pool get err:", err, "ActiveConn:", pool.AcivteConn(), _c.String())
		}
	}

}
func TestConnPool_GetNoWait(t *testing.T) {

	pool := GetDefPool(t)
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		_, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}

		if int32(i)+1 != pool.AcivteConn() {
			t.Fatal("AcivteConn ne index+1", i+1, pool.AcivteConn())
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())
	}
}

func TestConnPool_GetPut(t *testing.T) {
	pool := GetDefPool(t)
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		tran, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		if pool.AcivteConn() != 1 {
			t.Fatalf("AcivteConn[%v] ne 1", pool.AcivteConn())
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())

		tran.Close(false)
	}
}

func TestConnPool_forceClose(t *testing.T) {
	pool := GetDefPool(t)
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	for i := 0; i < 10; i++ {
		tran, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		if pool.AcivteConn() != 1 {
			t.Fatalf("AcivteConn[%v] ne 1", pool.AcivteConn())
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())

		tran.forceClose()
	}
}

func TestConnPool_MaxActive(t *testing.T) {
	t.Log("测试当达到最大MaxAcitve连接时,再Close时会关闭连接.")
	pool := GetDefPool(t)
	pool.MaxActive = 2
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}
	for i := 0; i < pool.MaxActive; i++ {
		_, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())
	}

	for i := 0; i < pool.MaxActive; i++ {
		tran, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())

		tran.Close(false)

		if pool.AcivteConn() != int32(pool.MaxActive) {
			t.Fatalf("close err MaxActive:%v != MaxActive:%v", pool.MaxActive, pool.MaxActive)
		}
	}
}

func TestConnPool_Close(t *testing.T) {
	pool := GetDefPool(t)
	pool.MaxActive = 1
	pool.Wait = true
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}
	for i := 0; i < pool.MaxActive; i++ {
		_, conn, err := GetConn(context.TODO(), pool)
		if err != nil {
			t.Fatal("pool get err:", err)
		}
		t.Log("pool Get:", conn.String(), "ActiveConn:", pool.AcivteConn())
	}

	go func() {
		_, _, err := GetConn(context.TODO(), pool)
		if err != ErrConnPoolClosed {
			t.Fatalf("pool get err:", err)
		} else {
			t.Log(" after pool close wait get res:", err)
		}
	}()

	pool.Close()

	for i := 0; i < pool.MaxActive; i++ {
		_, _, err := GetConn(context.TODO(), pool)
		if err != ErrConnPoolClosed {
			t.Fatal(" after close pool get err:", err)
		} else {
			t.Log(" after pool close get res:", err)
		}
	}
	time.Sleep(time.Second)
}

func TestConnPool_IdleTimeout(t *testing.T) {
	pool := GetDefPool(t)
	pool.MaxActive = 3
	pool.MaxIdle = 2
	pool.Wait = true
	pool.IdleTimeout = 2 * time.Second
	if err := pool.Init(); err != nil {
		t.Fatal("pool Init err:", err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 2*pool.MaxActive; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tran, _, err := GetConn(context.TODO(), pool)
			if err != nil {
				t.Fatal("pool get err:", err)
			}
			time.Sleep(time.Second)
			tran.Close(false)
		}()
	}

	wg.Wait()
	t.Log("AcivteConn:", pool.AcivteConn())

	time.Sleep(2 * pool.IdleTimeout)

	if pool.AcivteConn() != int32(pool.MaxIdle) {
		t.Fatalf("after IdleTimeout %T, AcivteConn:%v != MaxIdle:%v", pool.IdleTimeout, pool.AcivteConn(), pool.MaxIdle)
	} else {
		t.Logf("after IdleTimeout %T, AcivteConn:%v == MaxIdle:%v", pool.IdleTimeout, pool.AcivteConn(), pool.MaxIdle)
	}

}

func BenchmarkConnPool_Get(b *testing.B) {

	pool := &ConnPool{
		Dial: func(meta interface{}) (TClient, error) {
			n, _ := meta.(*int32)
			return &SocketConn{info: "test Dial", num: atomic.AddInt32(n, 1)}, nil
		},
		CloseConn: func(c TClient, meta interface{}) (err error) {
			b.Logf("CloseConn index:%d", c.(*SocketConn).num)
			return nil
		},
		UserMeta: &index,
	}
	pool.MaxActive = 10
	pool.MaxIdle = 10

	if err := pool.Init(); err != nil {
		b.Fatal("pool Init err:", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tran, _, err := GetConn(context.TODO(), pool)
		if err != nil {
			b.Fatal("pool get err:", err)
		}

		tran.Close(false)
	}
}
