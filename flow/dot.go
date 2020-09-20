package flow

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/grailbio/reflow/log"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
)

// Node is a flow node in the dot graph.
type Node struct {
	*Flow
}

// ID is the unique identifier for this node. Implements graph.Node
func (n Node) ID() int64 {
	i, err := strconv.ParseInt(n.Digest().Short(), 16, 64)
	if err != nil {
		log.Fatal(err)
	}
	return i
}

func lim64(s string) string {
	if len(s) <= 64 {
		return s
	}
	return strings.Join([]string{s[:30], "...", s[len(s)-30:]}, "")
}

// DOTID implements dot.Node.
func (n Node) DOTID() string {
	msg := fmt.Sprintf("%v-%v-%v", n.Digest().Short(), n.Op, n.Ident)
	if n.Op == Intern || n.Op == Extern {
		msg += fmt.Sprintf("-%v", lim64(n.URL.String()))
	}
	if n.ExecDepIncorrectCacheKeyBug {
		msg += "-ExecDepIncorrectCacheKeyBug"
	}
	return msg
}

// Attributes implments encoding.Attributer.
func (n Node) Attributes() []encoding.Attribute {
	var attrs []encoding.Attribute
	if n.Op.External() {
		switch n.ExecDepIncorrectCacheKeyBug {
		case true:
			attrs = append(attrs, encoding.Attribute{Key: "execdepincorrectcachekeybug", Value: "true"})
			attrs = append(attrs, encoding.Attribute{Key: "fillcolor", Value: "red"})
		case false:
			attrs = append(attrs, encoding.Attribute{Key: "fillcolor", Value: "green"})
		}
		attrs = append(attrs, encoding.Attribute{Key: "style", Value: "filled"})
	}
	return attrs
}

// Edge represents a dependency from a flow node to another.
type Edge struct {
	graph.Edge
	// dynamic indicates if this is dynamically explored edge.
	dynamic bool
}

// Attributes implments encoding.Attributer.
// Blue edges are dynamically explored edges.
func (e Edge) Attributes() []encoding.Attribute {
	var attrs []encoding.Attribute
	attrs = append(attrs, encoding.Attribute{Key: "dynamic", Value: fmt.Sprintf("%t", e.dynamic)})
	if e.dynamic {
		attrs = append(attrs, encoding.Attribute{Key: "color", Value: "blue"})
	}
	return attrs
}

// printDeps writes f's subgraph to the flowgraph.
func (e *Eval) printDeps(f *Flow, dynamic bool) {
	for _, v := range f.Deps {
		if !e.flowgraph.HasEdgeBetween(Node{Flow: f}.ID(), Node{Flow: v}.ID()) {
			e.flowgraph.SetEdge(Edge{Edge: e.flowgraph.NewEdge(Node{f}, Node{v}), dynamic: dynamic})
		}
		e.printDeps(v, dynamic)
	}
}
