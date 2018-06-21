package connpool

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrConnPoolClosed = errors.New("ConnPool: connpool closed")
	ErrDialNil        = errors.New("ConnPool: Dial func is nil")
	ErrCloseConnNil   = errors.New("ConnPool: CloseConn func is nil")
)

type TClient interface{}

type Transport struct {
	client   TClient
	lastPing time.Time
	lastUse  time.Time
	p        *ConnPool
}

func (tt *Transport) TC() TClient {
	return tt.client
}
func (tt *Transport) Close() error {
	return tt.p.put(tt, false)
}

func (tt *Transport) forceClose() error {
	return tt.p.put(tt, true)
}

type TransportArr []*Transport

func (arr TransportArr) Len() int {
	return len(arr)
}
func (arr TransportArr) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func (arr TransportArr) Less(i, j int) bool {
	return arr[i].lastUse.UnixNano() > arr[j].lastUse.UnixNano()
}

type ReqTran struct {
	tran *Transport
	do   chan *ReqTran
	err  error
	ctx  context.Context
}

func (req *ReqTran) done() {
	select {
	case req.do <- req:
	}
}

type ConnPool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	Dial func(meta interface{}) (TClient, error)

	// PingOnBorrow is an optional application supplied function for checking
	// the health of an idle connection
	Heartbeat func(c TClient, meta interface{}) error

	//连接关闭处理.
	CloseConn func(c TClient, meta interface{}) error

	// 连接失败时重试次数. 默认为1.
	DialRetryCount int

	IdleTimeout time.Duration

	//TODO 未实现.
	HeartbeatInterval time.Duration

	// 最大活跃连接数.
	MaxIdle int

	// 最大连接数.
	MaxActive int

	//当前打开的连接数.
	numOpen int32

	//当为true时, 超过最大连接数后会等带别的连接释放.
	Wait bool

	// 空闲连接队列.
	freeTran chan *Transport

	// 连接请求队列.
	reqTran chan *ReqTran

	// mu protects fields defined below.
	mu sync.Mutex

	closed bool

	closechan chan struct{}

	idleChan chan struct{}

	UserMeta interface{}
}

func (p *ConnPool) Init() (err error) {

	if p.Dial == nil {
		return ErrDialNil
	}
	if p.CloseConn == nil {
		return ErrCloseConnNil
	}
	//TODO
	//if p.Heartbeat != nil {
	//	go p.ping()
	//}

	if p.DialRetryCount <= 0 {
		p.DialRetryCount = 1
	}

	if p.MaxActive <= 0 {
		p.MaxActive = 1
	}

	if p.MaxIdle >= p.MaxActive {
		p.MaxIdle = p.MaxActive
	}

	p.closechan = make(chan struct{}, 1)
	p.freeTran = make(chan *Transport, 2*p.MaxActive)
	p.reqTran = make(chan *ReqTran, 5*p.MaxActive)
	p.idleChan = make(chan struct{}, 1)

	if p.IdleTimeout > 0 {
		go p.idelPro()
	}
	go p.allocationTran()

	return nil
}

func (p *ConnPool) ping() {

	//TODO Ping
}

//触发空闲处理.
func (p *ConnPool) idelPro() {
	t := time.NewTicker(p.IdleTimeout)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.idleChan <- struct{}{}
		case <-p.closechan:
			return
		}
	}
}

//负责分配连接.
func (p *ConnPool) allocationTran() {

	for {
		select {
		case req := <-p.reqTran: //需要一个连接.
		Loop:
			if len(p.freeTran) >= 1 || (p.MaxActive > 0 && atomic.LoadInt32(&p.numOpen) >= int32(p.MaxActive) && p.Wait) {
				select {
				case tran := <-p.freeTran:
					req.tran = tran
					req.done()
				case <-req.ctx.Done():
					req.err = errors.New(req.ctx.Err().Error())
					req.done()
				default:
					time.Sleep(time.Millisecond)
					goto Loop
				}
			} else {
				req.tran, req.err = p.conn()
				req.done()
			}
		case <-p.closechan:
			for req := range p.reqTran {
				req.err = ErrConnPoolClosed
				req.tran = nil
				req.done()
			}
			for len(p.freeTran) > 0 {
				select {
				case tran := <-p.freeTran:
					p.CloseConn(tran.client, p.UserMeta)
				}
			}
		case <-p.idleChan:
			//空闲处理. 取出freeTran的所有连接, lastUse比较, 关闭超过idelTime没有使用的连接.
			var _pArr []*Transport
			for len(p.freeTran) > 0 {
				_t := <-p.freeTran
				_pArr = append(_pArr, _t)
			}
			sort.Sort(TransportArr(_pArr))
			for _index, _v := range _pArr {
				if _index >= p.MaxIdle && time.Now().UnixNano()-_v.lastUse.UnixNano() > p.IdleTimeout.Nanoseconds() {
					p.CloseConn(_v.client, p.UserMeta)
					atomic.AddInt32(&p.numOpen, -1)
				} else {
					p.freeTran <- _v
				}
			}
		}
	}
}

func (p *ConnPool) Get(ctx context.Context) (tran *Transport, err error) {
	tran, err = p.get(ctx)
	if err != nil {
		return tran, err
	}
	tran.p = p
	return tran, nil
}
func (p *ConnPool) AcivteConn() int32 {
	return atomic.LoadInt32(&p.numOpen)
}

func (p *ConnPool) conn() (tran *Transport, err error) {

	if p.Dial == nil {
		return tran, ErrDialNil
	}

	atomic.AddInt32(&p.numOpen, 1)

	tran = &Transport{}
	var client TClient
	for i := 0; i < p.DialRetryCount; i++ {
		client, err = p.Dial(p.UserMeta)
		if err == nil {
			tran.client = client
			if p.Heartbeat != nil {
				err = p.Heartbeat(client, p.UserMeta)
				if err == nil {
					tran.lastPing = time.Now()
					break
				}
				p.CloseConn(client, p.UserMeta)
			} else {
				break
			}
		}
	}

	if err != nil {
		atomic.AddInt32(&p.numOpen, -1)
	}

	return tran, err
}

func (p *ConnPool) get(ctx context.Context) (tt *Transport, err error) {

	if p.closed {
		return tt, ErrConnPoolClosed
	}

	call := &ReqTran{
		do:  make(chan *ReqTran, 1),
		ctx: ctx,
	}
	//写入请求.
	p.reqTran <- call

	//等待分配结果.
	select {
	case r := <-call.do:
		err = r.err
		tt = r.tran
	}

	return tt, err
}

func (p *ConnPool) Close() error {

	p.closed = true
	p.closechan <- struct{}{}

	return nil
}

func (p *ConnPool) Put(tt *Transport) error {
	return p.put(tt, false)

}

func (p *ConnPool) put(tt *Transport, forceClose bool) (err error) {

	if tt == nil {
		return nil
	}
	if forceClose || (p.MaxActive > 0 && atomic.LoadInt32(&p.numOpen) > int32(p.MaxActive)) {
		atomic.AddInt32(&p.numOpen, -1)
		return p.CloseConn(tt.client, p.UserMeta)
	}

	tt.p = nil
	tt.lastUse = time.Now()
	p.freeTran <- tt

	return nil
}
