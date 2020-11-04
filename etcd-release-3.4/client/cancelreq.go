// borrowed from golang/net/context/ctxhttp/cancelreq.go

package client

import "net/http"

func requestCanceler(tr CancelableTransport, req *http.Request) func() {
	ch := make(chan struct{})
	req.Cancel = ch

	return func() {
		close(ch)
	}
}
