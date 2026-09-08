// TODO:
// - crear una lista de ficheros.
// - crear un struct de nodos := []node{parent, child}

// Package graph renders the dependencies between pipeline objects.
package graph

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/nicola-strappazzon/dacfy/pipelines"
	"github.com/spf13/cobra"
)

type node struct {
	name string
	outs []string
}

var sourceRE = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)`)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use: "graph <pipeline.yaml|directory> [...]", Short: "Print the pipeline dependency graph.",
		Example: "dacfy graph examples/splitview", Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error { return Run(cmd.OutOrStdout(), args) },
	}
}

// Run loads the supplied pipelines and writes their graph to out.
func Run(out io.Writer, paths []string) error {
	fmt.Println(GetFiles(paths[0]))

	files, err := expand(paths)
	if err != nil {
		return err
	}
	objects := map[string]bool{}
	loaded := make([]*pipelines.Pipelines, 0, len(files))
	for _, path := range files {
		p, err := load(path)
		if err != nil {
			return fmt.Errorf("load %s: %w", path, err)
		}
		loaded = append(loaded, p)
		if p.Table.Name.IsNotEmpty() {
			objects[p.Table.Name.ToString()] = true
		}
		if p.View.Name.IsNotEmpty() {
			objects[p.View.Name.ToString()] = true
		}
	}

	nodes := map[string]*node{}
	addNode := func(name string) {
		if name != "" && nodes[name] == nil {
			nodes[name] = &node{name: name}
		}
	}
	addEdge := func(from, to string) {
		from, to = baseName(from), baseName(to)
		if from == "" || to == "" || from == to {
			return
		}
		addNode(from)
		addNode(to)
		for _, existing := range nodes[from].outs {
			if existing == to {
				return
			}
		}
		nodes[from].outs = append(nodes[from].outs, to)
	}
	for _, p := range loaded {
		if p.Table.Name.IsNotEmpty() {
			name := p.Table.Name.ToString()
			addNode(name)
			for _, required := range p.Table.Require {
				addEdge(required, name)
			}
		}
		if p.View.Name.IsNotEmpty() {
			view := p.View.Name.ToString()
			addNode(view)
			for _, match := range sourceRE.FindAllStringSubmatch(p.View.Query.ToString(), -1) {
				if source := baseName(match[1]); objects[source] {
					addEdge(source, view)
				}
			}
			if p.View.To.IsNotEmpty() {
				addEdge(view, p.View.To.ToString())
			}
		}
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no tables or views found in the supplied pipelines")
	}
	_, err = fmt.Fprint(out, render(nodes))
	return err
}

func expand(paths []string) ([]string, error) {
	seen := map[string]bool{}
	var files []string
	var visit func(string) error
	visit = func(path string) error {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			paths, err := GetFiles(path)
			if err != nil {
				return err
			}
			for _, path := range paths {
				if err := visit(path); err != nil {
					return err
				}
			}
			return nil
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return err
		}
		if seen[path] {
			return nil
		}
		seen[path] = true
		p, err := load(path)
		if err != nil {
			return err
		}
		if len(p.Pipelines) > 0 {
			for _, child := range p.Pipelines {
				if err := visit(filepath.Join(filepath.Dir(path), child)); err != nil {
					return err
				}
			}
			return nil
		}
		files = append(files, path)
		return nil
	}
	for _, path := range paths {
		if err := visit(path); err != nil {
			return nil, err
		}
	}
	sort.Strings(files)
	return files, nil
}

// YAMLFiles lists the YAML files directly contained in dir.
func GetFiles(dir string) (files []string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".yaml") || strings.HasSuffix(entry.Name(), ".yml")) {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// load avoids pipelines.Load because a graph must not require credentials.
func load(path string) (*pipelines.Pipelines, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := &pipelines.Pipelines{}
	p.Config.Pipe = path
	if err := yaml.Unmarshal(data, p); err != nil {
		return nil, err
	}
	p.SetParents()
	return p, nil
}

func baseName(name string) string {
	name = strings.Trim(name, "`\"")
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[i+1:]
	}
	return name
}

func render(nodes map[string]*node) string {
	indegree := map[string]int{}
	for name := range nodes {
		indegree[name] = 0
	}
	for _, n := range nodes {
		sort.Strings(n.outs)
		for _, child := range n.outs {
			indegree[child]++
		}
	}
	var roots []string
	for name, degree := range indegree {
		if degree == 0 {
			roots = append(roots, name)
		}
	}
	sort.Strings(roots)

	visited := map[string]bool{}
	var out strings.Builder
	var write func(string, string)
	write = func(name, prefix string) {
		out.WriteString(name)
		visited[name] = true
		for i, child := range nodes[name].outs {
			if i == 0 {
				out.WriteString(" -> ")
			} else {
				out.WriteByte('\n')
				out.WriteString(prefix)
				out.WriteString(strings.Repeat(" ", len([]rune(name))+1))
				out.WriteString("-> ")
			}
			if visited[child] {
				out.WriteString(child)
			} else {
				write(child, prefix+strings.Repeat(" ", len([]rune(name))+4))
			}
		}
	}
	for _, root := range roots {
		write(root, "")
		out.WriteByte('\n')
	}
	// Cycles have no root, but should still be visible.
	var remaining []string
	for name := range nodes {
		if !visited[name] {
			remaining = append(remaining, name)
		}
	}
	sort.Strings(remaining)
	for _, name := range remaining {
		if !visited[name] {
			write(name, "")
			out.WriteByte('\n')
		}
	}
	return out.String()
}
