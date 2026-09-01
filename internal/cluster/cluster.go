package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lojadopocket/gostore/internal/erasure"
	"github.com/lojadopocket/gostore/internal/storage"
)

// Spec is the resolved cluster topology for this node.
type Spec struct {
	Disks      []erasure.Disk // full ordered list (local + remote), one erasure set
	LocalDisks []diskRPC      // this node's disks, in the order the RPC server addresses them
	PeerBases  []string       // other nodes' base URLs
	Self       string
}

// IsClusterArg reports whether an arg names a remote endpoint.
func IsClusterArg(a string) bool {
	return strings.HasPrefix(a, "http://") || strings.HasPrefix(a, "https://")
}

// Parse resolves `args` (each like http://host:port/data/d{1...4}) into a Spec
// from the point of view of the node whose base URL is `self`.
func Parse(args []string, self, secret string) (*Spec, error) {
	if self == "" {
		return nil, errors.New("cluster: GOSTORE_CLUSTER_SELF must be this node's base URL (e.g. http://node1:9000)")
	}
	if secret == "" {
		return nil, errors.New("cluster: GOSTORE_CLUSTER_SECRET must be set (shared across all nodes)")
	}
	self = strings.TrimRight(self, "/")

	sp := &Spec{Self: self}
	perPeer := map[string]int{}
	seenPeer := map[string]bool{}
	globalIdx := 0

	for _, a := range args {
		base, pathSpec, err := splitEndpoint(a)
		if err != nil {
			return nil, err
		}
		for _, p := range expandEllipsis(pathSpec) {
			diskOnPeer := perPeer[base]
			perPeer[base]++
			if base == self {
				d, err := storage.OpenLocalDisk(p, 0, globalIdx)
				if err != nil {
					return nil, fmt.Errorf("cluster: open local disk %s: %w", p, err)
				}
				sp.Disks = append(sp.Disks, d)
				sp.LocalDisks = append(sp.LocalDisks, d)
			} else {
				sp.Disks = append(sp.Disks, NewRemoteDisk(base, diskOnPeer, secret))
				if !seenPeer[base] {
					seenPeer[base] = true
					sp.PeerBases = append(sp.PeerBases, base)
				}
			}
			globalIdx++
		}
	}
	if len(sp.LocalDisks) == 0 {
		return nil, fmt.Errorf("cluster: none of the endpoints match GOSTORE_CLUSTER_SELF=%q", self)
	}
	if len(sp.Disks) < 4 || len(sp.Disks)%2 != 0 {
		return nil, fmt.Errorf("cluster: total disk count must be even and >= 4, got %d", len(sp.Disks))
	}
	return sp, nil
}

// splitEndpoint splits "http://host:port/data/dX" into ("http://host:port",
// "/data/dX").
func splitEndpoint(a string) (base, path string, err error) {
	scheme := "http://"
	if strings.HasPrefix(a, "https://") {
		scheme = "https://"
	} else if !strings.HasPrefix(a, "http://") {
		return "", "", fmt.Errorf("cluster: endpoint %q must start with http:// or https://", a)
	}
	rest := a[len(scheme):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return "", "", fmt.Errorf("cluster: endpoint %q has no disk path", a)
	}
	return scheme + rest[:slash], rest[slash:], nil
}

// expandEllipsis expands one {N...M} range in a path.
func expandEllipsis(p string) []string {
	o := strings.IndexByte(p, '{')
	c := strings.IndexByte(p, '}')
	if o < 0 || c < 0 || c < o {
		return []string{p}
	}
	inner := p[o+1 : c]
	i := strings.Index(inner, "...")
	if i < 0 {
		return []string{p}
	}
	lo, e1 := strconv.Atoi(strings.TrimSpace(inner[:i]))
	hi, e2 := strconv.Atoi(strings.TrimSpace(inner[i+3:]))
	if e1 != nil || e2 != nil || hi < lo {
		return []string{p}
	}
	prefix, suffix := p[:o], p[c+1:]
	var out []string
	for n := lo; n <= hi; n++ {
		out = append(out, fmt.Sprintf("%s%d%s", prefix, n, suffix))
	}
	return out
}
