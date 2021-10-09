package mantaray

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
)

type WalkLevelFunc func(nodeType int, path, prefix, hash []byte) error

const (
	File = iota
	Directory
)

func (n *Node) WalkLevel(ctx context.Context, root []byte, l Loader, level uint, walker WalkLevelFunc) error {
	node, err := n.LookupNode(ctx, root, l)
	if err != nil {
		return err
	}

	if node.IsValueType() {
		return nil
	}

	// path stack
	s := make([][]byte, 0)

	type nodeTag struct {
		*Node
		path []byte
		level uint
	}

	q := list.New()
	q.PushBack(&nodeTag{Node: node, path: []byte{}})

	var nextLevel func(n *Node, path, prefix []byte, cur uint) error

	nextLevel = func(n *Node, path, prefix []byte, cur uint) error {
		if cur > level {
			return nil
		}

		next, err := n.LookupNode(ctx, prefix, l)
		if err != nil {
			return err
		}

		if next.IsValueType() {
			//fmt.Printf("node value\n %s prefix %s\n", path, prefix)
			//fmt.Printf("is see directory %v\n", next.IsWithPathSeparatorType())

			curPath := path
			curPrefix := prefix

			if next.IsWithPathSeparatorType() {
				AfterSlash := bytes.IndexByte(prefix, byte(PathSeparator)) + 1
				curPath = make([]byte, len(path)+AfterSlash)
				copy(curPath, path)
				copy(curPath[len(path):], prefix[:AfterSlash])
				curPrefix = make([]byte, len(prefix)-AfterSlash)
				copy(curPrefix, prefix[AfterSlash:])
				if err := walker(Directory, curPath, []byte{}, nil); err != nil {
					return err
				}
				cur++
			}

			if cur > level {
				return nil
			}

			return walker(File, curPath, curPrefix, next.Reference())
		}

		path = append(path, prefix...)

		for _, fork := range next.forks {
			if fork.IsEdgeType() {
				if fork.prefix[len(fork.prefix)-1] != byte(PathSeparator) {
					prevPath := path
					curPrefix := fork.prefix
					if path[len(path)-1] != byte(PathSeparator) {
						beforeSlash := bytes.LastIndexByte(path, byte(PathSeparator))
						prevPath = make([]byte, beforeSlash+1)
						copy(prevPath, path[:beforeSlash+1])
						curPrefix = make([]byte, len(path)+len(fork.prefix)-beforeSlash-1)
						copy(curPrefix, path[beforeSlash+1:])
						copy(curPrefix[len(path)-beforeSlash-1:], fork.prefix)
					}
					if err := nextLevel(next, prevPath, curPrefix, cur + 1); err != nil {
						return err
					}
				} else {
					// TODO next level
					nextPath := make([]byte, len(path)+len(fork.prefix))
					copy(nextPath, path)
					copy(nextPath[len(path):], fork.prefix)
					fmt.Printf("next dir %s level %d\n", nextPath, cur + 1)
					if err := walker(Directory, nextPath, []byte{}, nil); err != nil {
						return err
					}
					if cur <= level {
						q.PushBack(&nodeTag{Node: fork.Node, path: nextPath, level: cur + 1})
					}
				}
			}
			if fork.IsValueType() {
				if fork.IsWithPathSeparatorType() {
					if err := nextLevel(next, path, fork.prefix, cur + 1); err != nil {
						return err
					}
				} else {
					// TODO
					//fmt.Printf("fork value\n %s prefix %s\n", path, prefix)
					lastSlash := bytes.LastIndexByte(prefix, byte(PathSeparator))
					if lastSlash != -1 {
						prefix = prefix[:0:0]
					}
					curPath := make([]byte, len(path)-len(prefix))
					copy(curPath, path[:len(path)-len(prefix)])
					curPrefix := make([]byte, len(prefix)+len(fork.prefix))
					copy(curPrefix, prefix)
					copy(curPrefix[len(prefix):], fork.prefix)
					if err := walker(File, curPath, curPrefix, fork.Reference()); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	for q.Len() > 0 {
		e := q.Front()
		q.Remove(e)
		t := e.Value.(*nodeTag)

		if t.forks == nil {
			if err := t.load(ctx, l); err != nil {
				return err
			}
		}

		for _, b := range t.forks {
			s = append(s, b.prefix)
		}

		end := len(s)
		for len(s) != 0 {
			cur := s[end-1]
			curLevel := t.level
			if cur[len(cur)-1] == byte(PathSeparator) {
				if curLevel <= level {
					curPath := make([]byte, len(t.path)+len(cur))
					copy(curPath, t.path)
					copy(curPath[len(t.path):], cur)
					if err := walker(Directory, curPath, []byte{}, nil); err != nil {
						return err
					}
				}
				curLevel++
			}
			//fmt.Printf("cur %s level %d\n", cur, curLevel)
			err := nextLevel(t.Node, t.path, cur, curLevel)
			if err != nil {
				return err
			}
			end--
			s = s[:end]
		}
	}

	return nil
}
