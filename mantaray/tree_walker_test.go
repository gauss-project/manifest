package mantaray

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
)

func TestWalkLevel(t *testing.T) {
	for _, tc := range []struct {
		name     string
		toAdd    [][]byte
		expected [][]byte
	}{
		{
			name: "simple",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/test/"),
				[]byte("img/test/oho.png"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds/person.jpg"),
				[]byte("img/test/ow/secret/.empty"),
				[]byte("robots.txt"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			expected: [][]byte{
				[]byte("index.html"),
				[]byte("img"),
				[]byte("img/test"),
				[]byte("img/test/oho.png"),
				[]byte("img/test/old"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds"),
				[]byte("img/test/olds/person.jpg"),
				[]byte("img/test/ow"),
				[]byte("img/test/ow/secret"),
				[]byte("img/test/ow/secret/.empty"),
				[]byte("robots.txt"),
				[]byte("robot"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src"),
				[]byte("src/logo.gif"),
				[]byte("src/default"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults"),
				[]byte("src/defaults/1"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			walkedCount := 0

			walker := func(nodeType int, path, prefix, hash []byte) error {
				walkedCount++

				pathFound := false

				fullPath := make([]byte, len(path)+len(prefix))
				copy(fullPath, path)
				copy(fullPath[len(path):], prefix)

				fullPath = bytes.TrimSuffix(fullPath, []byte{byte(PathSeparator)})

				for i := 0; i < len(tc.expected); i++ {
					c := tc.expected[i]
					if bytes.Equal(fullPath, c) {
						pathFound = true
						break
					}
				}

				if !pathFound {
					return fmt.Errorf("walkFn returned unknown path: %s", fullPath)
				}

				return nil
			}
			// Expect no errors.
			err := n.WalkLevel(ctx, []byte{}, nil, math.MaxUint64, walker)
			if err != nil {
				t.Fatalf("no error expected, found: %s", err)
			}

			if len(tc.expected) != walkedCount {
				t.Errorf("expected %d nodes, got %d", len(tc.expected), walkedCount)
			}

		})
	}
}
