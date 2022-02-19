package delayed_queue

import (
	"fmt"
	"sync"
	"time"
)

type ChanQueue struct {
	Data        []interface{}
	Mutex       *sync.Mutex
	timeSpy     bool //whether drop data out of time. timeSpy should be set only at beginning and unmodifiable
	chanSpy     bool
	ExpireAfter time.Duration
	flag        chan int      //use this to stop time spy for a time queue
	cap         int           //queue extends minimum step
	timeStep    time.Duration //how frequency is the spy routine runs by default 10s
}
type ChanWrapper struct {
	Data           interface{}
	CreatedAt      int64
	ChanArrive     chan struct{}
	PopArrive      chan struct{}
	Func           func()
	ChanArriveFlag bool
}

func ChanQueueWithTimeStep(expireAfter time.Duration, cap int, tsp time.Duration) *ChanQueue {
	return &ChanQueue{
		Data:        make([]interface{}, 0, cap),
		timeSpy:     true,
		chanSpy:     true,
		ExpireAfter: expireAfter,
		cap:         cap,
		timeStep:    tsp,
		Mutex:       &sync.Mutex{},
	}
}

func (q *ChanQueue) ChanValidHead() (*ChanWrapper, int) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	if len(q.Data) == 0 {
		return &ChanWrapper{}, -1
	}
	for i := 0; i < len(q.Data); i++ {
		if q.Data[i] != nil {
			if t, ok := q.Data[i].(*ChanWrapper); ok {
				if t.ChanArriveFlag {
					continue
				}
				return t, i
			}

		}
	}
	return &ChanWrapper{}, -1
}

func (q *ChanQueue) tHead() (*ChanWrapper, int, error) {
	if len(q.Data) == 0 {
		return nil, -1, fmt.Errorf("empty queue")
	}

	tw := q.Data[0].(*ChanWrapper)
	return tw, 0, nil

}

func (q *ChanQueue) SafeTHead() (interface{}, int, error) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	r, i, err := q.tHead()
	return r.Data, i, err
}

//the last not nil value
func (q *ChanQueue) ValidTail() (interface{}, int) {
	if len(q.Data) == 0 {
		return nil, -1
	}
	for i := len(q.Data) - 1; i >= 0; i-- {
		if q.Data[i] != nil {
			return q.Data[i].(*ChanWrapper).Data, i
		}
	}
	return nil, -1
}

//push a value in queue
func (q *ChanQueue) chanPush(data interface{}, DieChan chan struct{}, popChan chan struct{}, f func()) *ChanWrapper {

	dataC := &ChanWrapper{
		Data:           data,
		CreatedAt:      time.Now().Unix(),
		Func:           f,
		ChanArriveFlag: false,
		ChanArrive:     DieChan,
		PopArrive:      popChan,
	}

	if rs, _ := q.ValidTail(); rs != nil || len(q.Data) == 0 {
		q.Data = append(q.Data, dataC)
	} else {
		q.Data[len(q.Data)-1] = dataC
	}
	_, i := q.ValidHead()
	q.Data = q.Data[i:]
	return dataC
}

func (q *ChanQueue) ValidHead() (interface{}, int) {
	if len(q.Data) == 0 {
		return nil, -1
	}
	for i := 0; i < len(q.Data); i++ {
		if q.Data[i] != nil {

			return q.Data[i].(*ChanWrapper).Data, i
		}
	}
	return nil, -1
}

func (q *ChanQueue) ChanHead() (*ChanWrapper, int) {
	if len(q.Data) == 0 {
		return nil, -1
	}
	wrapper, index := q.Data[0], 0
	return wrapper.(*ChanWrapper), index
}

func (q *ChanQueue) ChanPush(data interface{}, DieChan chan struct{}, fTimeOut func(), fDie func()) {
	popChan := make(chan struct{})
	ele := q.chanPush(data, DieChan, popChan, fTimeOut)
	go func(ele *ChanWrapper) {
		select {
		case <-ele.ChanArrive:

			ele.ChanArriveFlag = true
			fmt.Println("ele", ele)
			fmt.Println("ele.ChanArriveFlag = true")
		case <-ele.PopArrive:
		}
		fDie()
	}(ele)
}

func (q *ChanQueue) ChanPop() interface{} {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	rs, index := q.ChanValidHead()
	if index >= 0 {
		q.Data = q.Data[index+1:]
		rs.PopArrive <- struct{}{}
		return rs.Data
	}
	q.Data = make([]interface{}, 0, q.cap)
	return nil
}

func (q *ChanQueue) weed() *ChanWrapper {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	rs, index := q.ChanHead()
	if len(q.Data) > 1 {
		q.Data = q.Data[index+1:]
	} else {
		q.Data = make([]interface{}, 0, q.cap)
	}
	rs.PopArrive <- struct{}{}
	return rs
}

//print this queue
func (q *ChanQueue) Print() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	fmt.Println("<-out")
	for _, i := range q.Data {
		fmt.Printf("%v", i)
	}
	fmt.Println("<-in")
}

func (q *ChanQueue) SafeLength() int {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	return len(q.Data)
}

// start to spy on queue's time-out data and throw it
func (q *ChanQueue) StartTimeSpying() {
	go func() {
		err := q.startTimeSpying()
		if err != nil {

		}
	}()
}

// detail of StartTimeSpying function
func (q *ChanQueue) startTimeSpying() error {

	var err = make(chan string)
	go func(queue *ChanQueue, er chan string) {
		fmt.Println("start time spying, data in the queue can stay for " + q.ExpireAfter.String())
		for {
			if !queue.timeSpy {
				err <- "spying routine stops because: queue's timeSpy is false, make sure the queue is definition by q=TimeQueue(time.Duration,int)"
				return
			}
			select {
			case <-queue.flag:
				fmt.Println("time spy executing stops")
				return
			default:
				fmt.Print()
			}
			ok, er := queue.timingRemove()
			if er != nil {
				err <- er.Error()
			}
			if ok {
				time.Sleep(queue.timeStep)
			}
		}
	}(q, err)
	select {
	case msg := <-err:
		fmt.Println("time spy supervisor accidentally stops because: ", msg)
		return fmt.Errorf(msg)
	case <-q.flag:
		fmt.Println("time spy supervisor stops")
		return nil
	}
}

//stop supervisor and execution of time spying
func (q *ChanQueue) StopTimeSpying() {
	close(q.flag)
}

// remove those time-out data
func (q *ChanQueue) timingRemove() (bool, error) {
	if len(q.Data) < 1 {
		return true, nil
	}
	head, index, er := q.tHead()
	if er != nil {
		return false, er
	}
	if index < 0 {
		return false, fmt.Errorf("queue'length goes 0")
	}
	now := time.Now().Unix()
	created := time.Unix(head.CreatedAt, 0)
	//fmt.Println("now:",now)
	//fmt.Println("expire:",created.Add(q.ExpireAfter).Unix())
	if created.Add(q.ExpireAfter).Unix() < now {
		// out of time
		e := q.weed()
		fmt.Println(q.ExpireAfter, created)
		go e.Func()

		if e != nil {
			return false, nil
		}
		if len(q.Data) > 0 {
			return q.timingRemove()
		}
		return true, nil
	}
	return true, nil
}
