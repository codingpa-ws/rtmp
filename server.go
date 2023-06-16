package rtmp

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/codingpa-ws/rtmp/constants"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Server represents the RTMP server, where a client/app can stream media to. The server listens for incoming connections.
type Server struct {
	AppName     string
	Addr        string
	Logger      *zap.Logger
	Broadcaster Broadcaster
}

// Listen starts the server and listens for any incoming connections. If no Addr (host:port) has been assigned to the server, ":1935" is used.
func (s *Server) Listen() error {
	if s.Addr == "" {
		s.Addr = constants.DefaultAddress
	}

	tcpAddress, err := net.ResolveTCPAddr("tcp", s.Addr)
	if err != nil {
		err = errors.Errorf("[server] error resolving tcp address: %s", err)
		return err
	}

	// Start listening on the specified address
	listener, err := net.ListenTCP("tcp", tcpAddress)
	if err != nil {
		return err
	}

	s.Logger.Info(fmt.Sprint("[server] Listening on ", s.Addr))

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.Logger.Error(fmt.Sprint("[server] Error accepting incoming connection ", err))
			continue
		}

		s.Logger.Info(fmt.Sprint("[server] Accepted incoming connection from ", conn.RemoteAddr().String()))

		go func(conn net.Conn) {
			defer conn.Close()

			socketr := bufio.NewReaderSize(conn, constants.BuffioSize)
			socketw := bufio.NewWriterSize(conn, constants.BuffioSize)
			sess := NewSession(s.Logger, s.Broadcaster)

			sess.messageManager = NewMessageManager(sess,
				NewHandshaker(socketr, socketw),
				NewChunkHandler(socketr, socketw),
			)

			s.Logger.Info(fmt.Sprint("[server] Starting server session with sessionId ", sess.id))
			err := sess.Start()
			if err != io.EOF {
				s.Logger.Error(fmt.Sprint("[server] Server session with sessionId ", sess.id, " ended with an error: ", err))
			} else {
				s.Logger.Info(fmt.Sprint("[server] Server session with sessionId ", sess.id, " ended."))
			}
		}(conn)

	}

}
