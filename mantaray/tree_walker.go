package mantaray

import (
	"bytes"
	"container/list"
	"context"
	"errors"
)

type WalkLevelFunc func(nodeType int, path, prefix, hash []byte) error

const (
	File = iota
	Directory
)

var (
	emptyPath = []byte{}
	errMaxHeight = errors.New("reach maximum directory level")
)

type nodeTag struct {
	*Node
	path []byte
	subPath []byte
	level uint
}

func (n *Node) lookupClosest(ctx context.Context, path []byte, l Loader) (*Node, []byte, error) {
	select {
	case <-ctx.Done():
		return nil, path, ctx.Err()
	default:
	}
	if n.forks == nil {
		if err := n.load(ctx, l); err != nil {
			return nil, path, err
		}
	}
	if len(path) == 0 {
		return n, path, nil
	}
	f := n.forks[path[0]]
	if f == nil {
		return n, path, nil
	}
	c := common(f.prefix, path)
	if len(c) == len(f.prefix) {
		return f.Node.lookupClosest(ctx, path[len(c):], l)
	}
	return n, path, nil
}

func (n *Node) WalkLevel(ctx context.Context, root []byte, l Loader, level uint, walker WalkLevelFunc) error {
	node, remain, err := n.lookupClosest(ctx, root, l)
	if err != nil {
		return err
	}

	// path stack
	s := make([][]byte, 0)

	if len(root) != 0 {
		lastSlash := bytes.LastIndexByte(root, byte(PathSeparator))
		if lastSlash != -1 {
			root = root[lastSlash+1:]
		}
		root = bytes.TrimSuffix(root, remain)
		remain = append(remain, byte(PathSeparator))
	}

	q := list.New()
	q.PushBack(&nodeTag{Node: node, path: root, subPath: emptyPath})

	var (
		nextLevel func(n *Node, path, sub, prefix []byte, cur uint) error
		readDirectory func(fn WalkLevelFunc, start int, path []byte, pCur *uint) (int, error)
	)

	readDirectory = func(fn WalkLevelFunc, start int, path []byte, pCur *uint) (int, error) {
		i := start
		for j := i; j < len(path); j++ {
			if path[j] == byte(PathSeparator) {
				copyPath := make([]byte, j+1)
				copy(copyPath, path[:j+1])
				if err := walker(Directory, copyPath, []byte{}, nil); err != nil {
					return j, err
				}
				i = j + 1
				*pCur++
			}
			if *pCur > level {
				return i, errMaxHeight
			}
		}
		return i, nil
	}

	nextLevel = func(n *Node, path, sub, prefix []byte, cur uint) error {
		if cur > level {
			return nil
		}

		next, err := n.LookupNode(ctx, prefix, l)
		if err != nil {
			return err
		}

		prefix = append(sub, prefix...)

		lastPathSlash := bytes.LastIndexByte(path, byte(PathSeparator)) + 1
		lastPrefixSlash := bytes.LastIndexByte(prefix, byte(PathSeparator))

		if next.IsValueType() && lastPrefixSlash < len(prefix) - 1 {
			curPath := path
			curPrefix := prefix

			if lastPrefixSlash != -1 {
				curPath = append(curPath, prefix...)
				idx, err := readDirectory(walker, lastPathSlash, curPath, &cur)
				if err != nil {
					if errors.Is(err, errMaxHeight) {
						return nil
					}
					return err
				}
				curPrefix = curPath[idx:]
				curPath = curPath[:idx]
			}

			if err := walker(File, curPath, curPrefix, next.Reference()); err != nil {
				return err
			}
		}

		if !next.IsEdgeType() {
			return nil
		}

		path = append(path, prefix...)

		for _, fork := range next.forks {
			storeCur := cur

			if fork.IsValueType() {
				if fork.IsWithPathSeparatorType() {
					if err := nextLevel(next, path, emptyPath, fork.prefix, storeCur + 1); err != nil {
						return err
					}
					continue
				} else {
					lastForkSlash := bytes.LastIndexByte(fork.prefix, byte(PathSeparator))
					curPath := make([]byte, len(path))
					copy(curPath, path)
					curPrefix := make([]byte, len(prefix))
					copy(curPrefix, prefix)
					curPrefix = curPrefix[lastPrefixSlash+1:]
					if lastForkSlash != -1 {
						curPath = append(curPath, fork.prefix[:lastForkSlash+1]...)
						idx, err := readDirectory(walker, lastPathSlash, curPath, &storeCur)
						if err != nil {
							if errors.Is(err, errMaxHeight) {
								continue
							}
							return err
						}
						curPath = curPath[:idx]
						curPrefix = fork.prefix[lastForkSlash+1:]
					} else {
						curPath = bytes.TrimSuffix(curPath, curPrefix)
						curPrefix = append(curPrefix, fork.prefix...)
					}
					if err := walker(File, curPath, curPrefix, fork.Reference()); err != nil {
						return err
					}
				}
			}

			if fork.IsEdgeType() {
				if next.IsWithPathSeparatorType() {
					idx, err := readDirectory(walker, lastPathSlash, path, &storeCur)
					if err != nil {
						if errors.Is(err, errMaxHeight) {
							continue
						}
						return err
					}
					lastPathSlash = idx
				}
				var curPath, subPath []byte
				dirSlash := bytes.LastIndexByte(fork.prefix, byte(PathSeparator))
				if dirSlash != -1 {
					copyPath := make([]byte, len(path)+dirSlash+1)
					copy(copyPath, path)
					copy(copyPath[len(path):], fork.prefix[:dirSlash+1])
					idx, err := readDirectory(walker, lastPathSlash, copyPath, &storeCur)
					if err != nil {
						if !errors.Is(err, errMaxHeight) {
							return err
						}
					}
					curPath = copyPath[:idx]
					subPath = fork.prefix[dirSlash+1:]
				} else {
					curPath = path[:lastPathSlash]
					subPath = make([]byte, len(path)+len(fork.prefix)-lastPathSlash)
					copy(subPath, path[lastPathSlash:])
					copy(subPath[len(path)-lastPathSlash:], fork.prefix)
				}
				if storeCur <= level {
					q.PushBack(&nodeTag{Node: fork.Node, path: curPath, subPath: subPath, level: storeCur + 1})
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

		l := t.level
		path := append(t.path, t.subPath...)

		lastPathSlash := bytes.LastIndexByte(path, byte(PathSeparator)) + 1

		idx, err := readDirectory(walker, lastPathSlash, path, &l)
		if err != nil {
			if !errors.Is(err, errMaxHeight) {
				return err
			}
		}

		end := len(s)
		for len(s) != 0 {
			prefix := s[end-1]
			couldNext := true
			if len(remain) > 0 && t.level == 0 && !bytes.HasPrefix(prefix, remain) {
				couldNext = false
			}
			if couldNext {
				if err := nextLevel(t.Node, path[:idx], path[idx:], prefix, t.level); err != nil {
					return err
				}
			}
			end--
			s = s[:end]
		}
	}

	return nil
}
