package gracegrpc

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/facebookgo/grace/gracenet"
	"google.golang.org/grpc"
)

var (
	didInherit = os.Getenv("LISTEN_FDS") != ""
	ppid       = os.Getppid()
)

type GraceGrpc struct {
	server   *grpc.Server
	net      *gracenet.Net
	listener net.Listener
	errors   chan error
	pidfile  string
	OnClose  func()
}

func New(s *grpc.Server, net, addr, pidFile string) (*GraceGrpc, error) {
	gr := &GraceGrpc{
		server: s,
		net:    &gracenet.Net{},

		//for  StartProcess error.
		errors:  make(chan error),
		pidfile: pidFile,
	}
	l, err := gr.net.Listen(net, addr)
	if err != nil {
		return nil, err
	}
	gr.listener = l
	return gr, nil
}

func (gr *GraceGrpc) serve() {
	go func() {
		if err := gr.server.Serve(gr.listener); err != nil {
			log.Println(err.Error())
		}
	}()
}

func (gr *GraceGrpc) wait() {
	var wg sync.WaitGroup
	wg.Add(1)
	go gr.signalHandler(&wg)
	wg.Wait()
}

func (gr *GraceGrpc) signalHandler(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			defer wg.Done()
			signal.Stop(ch)
			gr.server.GracefulStop()
			return
		case syscall.SIGUSR2:
			gr.OnClose()
			if _, err := gr.net.StartProcess(); err != nil {
				gr.errors <- err
			}
		}
	}
}

func (gr *GraceGrpc) doWritePid(pid int) error {
	if gr.pidfile == "" {
		return fmt.Errorf("No pid file path")
	}

	pf, err := os.Create(gr.pidfile)
	defer pf.Close()
	if err != nil {
		return err
	}

	_, err = pf.WriteString(strconv.Itoa(pid))
	if err != nil {
		return err
	}
	return err
}

func (gr *GraceGrpc) Serve() error {

	if didInherit {
		if ppid == 1 {
			log.Printf("Listening on init activated %s\n", pprintAddr(gr.listener))
		} else {
			const msg = "Graceful handoff of %s with new pid %d replace old pid %d"
			log.Printf(msg, pprintAddr(gr.listener), os.Getpid(), ppid)
		}
	} else {
		const msg = "Serving %s with pid %d\n"
		log.Printf(msg, pprintAddr(gr.listener), os.Getpid())
	}

	err := gr.doWritePid(os.Getpid())
	if err != nil {
		return err
	}

	gr.serve()

	if didInherit && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to close parent: %s", err)
		}
	}

	waitdone := make(chan struct{})
	go func() {
		defer close(waitdone)
		gr.wait()
	}()

	select {
	case err := <-gr.errors:
		return err
	case <-waitdone:

		log.Printf("Exiting pid %d.", os.Getpid())
		return nil
	}
}

func pprintAddr(l net.Listener) []byte {
	var out bytes.Buffer
	fmt.Fprint(&out, l.Addr())
	return out.Bytes()
}
