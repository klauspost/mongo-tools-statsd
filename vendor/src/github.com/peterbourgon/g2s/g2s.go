package g2s

import (
	"io"
	"log"
	"math/rand"
	"net"
	"time"
	"sync"
)

const (
	MAX_PACKET_SIZE = 65536 - 8 - 20 // 8-byte UDP header, 20-byte IP header
)

type Statter interface {
	Counter(sampleRate float32, bucket string, n ...int)
	Timing(sampleRate float32, bucket string, d ...time.Duration)
	Gauge(sampleRate float32, bucket string, value ...string)
}

type statsd struct {
	w io.Writer
	proto string
	endpoint string
	timeout time.Duration
	lock sync.RWMutex // On message sends, only a read lock will be requested, but on reconnects a write lock will protect it.
}


// Dial takes the same parameters as net.Dial, ie. a transport protocol
// (typically "udp") and an endpoint. It returns a new Statsd structure,
// ready to use. Timeout is 2 seconds.
//
// g2s will attemt to reconnect on write failures
func Dial(proto, endpoint string) (Statter, error) {	
	c, err := net.DialTimeout(proto, endpoint, time.Second * 2)
	if err != nil {
		return nil, err
	}
	return &statsd {
		w: c,
		proto: proto,
		endpoint: endpoint,
		timeout: time.Second * 2,
	}, nil
}

// DialTimeout takes the same parameters as net.DialTimeout, ie. a transport protocol
// (typically "udp") and an endpoint, as well as a timeout.
// It returns a new Statsd structure, ready to use.
// 
//
// g2s will attemt to reconnect on write failures.
func DialTimeout(proto, endpoint string, timeout time.Duration) (Statter, error) {	
	c, err := net.DialTimeout(proto, endpoint, timeout)
	if err != nil {
		return nil, err
	}
	return &statsd {
		w: c,
		proto: proto,
		endpoint: endpoint,
		timeout: timeout,
	}, nil
}


// New constructs a Statsd structure which will write statsd-protocol messages
// into the given io.Writer. New is intended to be used by consumers who want
// nonstandard behavior: for example, they may pass an io.Writer which performs
// buffering and aggregation of statsd-protocol messages.
//
// Note that g2s provides no synchronization. If you pass an io.Writer which
// is not goroutine-safe, for example a bytes.Buffer, you must make sure you
// synchronize your calls to the Statter methods.
func New(w io.Writer) (Statter, error) {
	return &statsd{
		w: w,
	}, nil
}

// bufferize folds the slice of sendables into a slice of byte-buffers,
// each of which shall be no larger than max bytes.
func bufferize(sendables []sendable, max int) [][]byte {
	bN := [][]byte{}
	b1, b1sz := []byte{}, 0

	for _, sendable := range sendables {
		buf := []byte(sendable.Message())
		if b1sz+len(buf) > max {
			bN = append(bN, b1)
			b1, b1sz = []byte{}, 0
		}
		b1 = append(b1, buf...)
		b1sz += len(buf)
	}

	if len(b1) > 0 {
		bN = append(bN, b1[0:len(b1)-1])
	}

	return bN
}


// publish folds the slice of sendables into one or more packets, each of which
// will be no larger than MAX_PACKET_SIZE. It then writes them, one by one,
// into the Statsd io.Writer.
func (s *statsd) publish(msgs []sendable) {
	s.lock.RLock()

	for _, buf := range bufferize(msgs, MAX_PACKET_SIZE) {
		// In the base case, when the Statsd struct is backed by a net.Conn,
		// "Multiple goroutines may invoke methods on a Conn simultaneously."
		//   -- http://golang.org/pkg/net/#Conn
		// Otherwise, Bring Your Own Synchronization™.
		n, err := s.w.Write(buf)
		if err != nil && s.endpoint == "" {
			log.Printf("g2s: publish: %s", err)
		} else if err != nil {
			// Attempt one reconnect before failing
			s.lock.RUnlock()
			err = s.reconnect()
			s.lock.RLock()
			if err != nil {
				log.Printf("g2s: reconnect: %s", err)
			}
			n, err = s.w.Write(buf)
			if err != nil {
				log.Printf("g2s: publish: %s (after reconnect)", err)
			}
		} else if n != len(buf) {
			log.Printf("g2s: publish: short send: %d < %d", n, len(buf))
		}
	}
	s.lock.RUnlock()
}

// reconnect will attempt to reconnect the original connection after a write failure.
// 
// The mutex will be locked for writing, and incoming attempts will be blocked until
// the new connection has been stored.
func (s *statsd) reconnect() (err error) {
	s.lock.Lock()
	log.Printf("g2s: Reconnecting")
	s.w, err = net.DialTimeout(s.proto, s.endpoint, s.timeout)
	s.lock.Unlock()
	return
}

// maybeSample returns a sampling structure and true if a pseudorandom number
// in the range 0..1 is less than or equal to the passed rate.
//
// As a special case, if r >= 1.0, maybeSample will return an uninitialized
// sampling structure and true. The uninitialized sampling structure implies
// enabled == false, which tells statsd that the value is unsampled.
func maybeSample(r float32) (sampling, bool) {
	if r >= 1.0 {
		return sampling{}, true
	}

	if rand.Float32() > r {
		return sampling{}, false
	}

	return sampling{
		enabled: true,
		rate:    r,
	}, true
}

// Counter sends one or more counter statistics to statsd.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *statsd) Counter(sampleRate float32, bucket string, n ...int) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(n))
	for i, ni := range n {
		msgs[i] = &counterUpdate{
			bucket:   bucket,
			n:        ni,
			sampling: samp,
		}
	}

	s.publish(msgs)
}

// Timing sends one or more timing statistics to statsd.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *statsd) Timing(sampleRate float32, bucket string, d ...time.Duration) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(d))
	for i, di := range d {
		msgs[i] = &timingUpdate{
			bucket:   bucket,
			ms:       int(di.Nanoseconds() / 1e6),
			sampling: samp,
		}
	}

	s.publish(msgs)
}

// Gauge sends one or more gauge statistics to statsd.
//
// Application code should call it for every potential invocation of a
// statistic; it uses the sampleRate to determine whether or not to send or
// squelch the data, on an aggregate basis.
func (s *statsd) Gauge(sampleRate float32, bucket string, v ...string) {
	samp, ok := maybeSample(sampleRate)
	if !ok {
		return
	}

	msgs := make([]sendable, len(v))
	for i, vi := range v {
		msgs[i] = &gaugeUpdate{
			bucket:   bucket,
			val:      vi,
			sampling: samp,
		}
	}

	s.publish(msgs)
}
