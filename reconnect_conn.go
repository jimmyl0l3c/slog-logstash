package sloglogstash

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var _ net.Conn = (*ReconnectConn)(nil)

type ReconnectConn struct {
	dialer        func() (net.Conn, error) // Function to dial new connection
	conn          net.Conn                 // Current active connection
	mu            sync.Mutex               // Mutex to protect conn
	writeDeadline time.Time                // Deadline for future write calls

	retryDelay time.Duration // Delay between reconnect attempts
	maxRetries int           // Maximum number of reconnect attempts, infinite by default

	closing   atomic.Bool // Indicates if the connection is closing/closed (atomic)
	closeOnce sync.Once   // Ensures close is only performed once
}

func NewReconnectConn(dialer func() (net.Conn, error), retryDelay time.Duration) *ReconnectConn {
	return &ReconnectConn{
		dialer: dialer,
		mu:     sync.Mutex{},

		retryDelay: retryDelay,
	}
}

// SetMaxRetries sets the maximum number of connection attempts.
// You can make it unlimited by setting it to zero (the default).
func (rc *ReconnectConn) SetMaxRetries(maxRetries int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.maxRetries = maxRetries
}

func (rc *ReconnectConn) ensureConn() error {
	if rc.conn != nil {
		return nil
	}

	var err error
	for i := 0; rc.maxRetries == 0 || i < rc.maxRetries; i++ {
		if rc.closing.Load() {
			return errors.New("Connection is closed.")
		}

		if i > 0 {
			time.Sleep(rc.retryDelay)
		}

		rc.conn, err = rc.dialer()
		if err == nil {
			_ = rc.conn.SetWriteDeadline(rc.writeDeadline)
			return nil
		}
	}

	return err
}

// Implements net.Conn.
func (rc *ReconnectConn) Read(b []byte) (n int, err error) {
	return -1, errors.New("ReconnectConn is a write only connection.")
}

// Implements net.Conn.
func (rc *ReconnectConn) Write(b []byte) (n int, err error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if err := rc.ensureConn(); err != nil {
		return 0, err
	}

	n, err = rc.conn.Write(b)
	if err != nil {
		_ = rc.conn.Close()
		rc.conn = nil
	}

	return n, err
}

// Implements net.Conn.
func (rc *ReconnectConn) Close() error {
	var err error
	rc.closeOnce.Do(func() {
		rc.closing.Store(true)
		rc.mu.Lock()
		defer rc.mu.Unlock()

		if rc.conn != nil {
			err = rc.conn.Close()
			rc.conn = nil
		}
	})
	return err
}

// Implements net.Conn.
func (rc *ReconnectConn) LocalAddr() net.Addr {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if err := rc.ensureConn(); err != nil {
		return nil
	}
	return rc.conn.LocalAddr()
}

// Implements net.Conn.
func (rc *ReconnectConn) RemoteAddr() net.Addr {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if err := rc.ensureConn(); err != nil {
		return nil
	}
	return rc.conn.RemoteAddr()
}

// Implements net.Conn.
func (rc *ReconnectConn) SetDeadline(t time.Time) error {
	return rc.SetWriteDeadline(t)
}

// Implements net.Conn.
func (rc *ReconnectConn) SetReadDeadline(t time.Time) error {
	return errors.New("ReconnectConn is a write only connection.")
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (rc *ReconnectConn) SetWriteDeadline(t time.Time) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.writeDeadline = t

	if err := rc.ensureConn(); err != nil {
		return err
	}

	return rc.conn.SetWriteDeadline(t)
}
