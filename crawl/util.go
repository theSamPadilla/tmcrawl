package crawl

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/harwoeck/ipstack"
	"github.com/rs/zerolog/log"

	//rpcclient "github.com/tendermint/tendermint/rpc/client"
	httpclient "github.com/tendermint/tendermint/rpc/client/http"
	//jsonrpcclient "github.com/tendermint/tendermint/rpc/jsonrpc/client"
	//libclient "github.com/tendermint/tendermint/rpc/lib/client"
)

func newRPCClient(remote string) *httpclient.HTTP {
	//Add "tcp" to remote
	var remoteAddr = fmt.Sprintf("tcp://%s", remote)
	httpClient, err := httpclient.NewWithTimeout(remoteAddr, "/websocket", 2)
	if err != nil {
		log.Err(err).Str("Failed to create json RPC client with address", remoteAddr)
	}

	// //Add timeout and return rpc client
	// httpClient.Timeout = clientTimeout
	// rpcClient := jsonrpcclient.Client().NewWithHTTPClient(remoteAddr, httpClient)
	return httpClient
}

func parsePort(nodeAddr string) string {
	u, err := url.Parse(nodeAddr)
	if err != nil {
		return ""
	}

	return u.Port()
}

func parseHostname(nodeAddr string) string {
	//fmt.Println("Parsing hostname", nodeAddr)
	u, err := url.Parse(nodeAddr)
	if err != nil {
		//fmt.Println("Returning ERROR", err)
		return ""
	}

	fmt.Println(u.Host)
	var list = strings.Split(nodeAddr, "@")
	var host = list[len(list)-1]

	return host
	//return u.Hostname()
}

func locationFromIPResp(r *ipstack.Response) Location {
	return Location{
		Country:   r.CountryName,
		Region:    r.RegionName,
		City:      r.City,
		Latitude:  fmt.Sprintf("%f", r.Latitude),
		Longitude: fmt.Sprintf("%f", r.Longitude),
	}
}

// PingAddress attempts to ping a P2P Tendermint address returning true if the
// node is reachable and false otherwise.
func PingAddress(address string, t int64) bool {
	conn, err := net.DialTimeout("tcp", address, time.Duration(t)*time.Second)
	if err != nil {
		return false
	}

	defer conn.Close()
	return true
}
