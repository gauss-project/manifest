// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mantaray

import (
	"bytes"
	"errors"
	"testing"
)

func TestNilPath(t *testing.T) {
	n := New()
	_, err := n.Lookup(nil, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestAddAndLookup(t *testing.T) {
	n := New()
	testCases := [][]byte{
		[]byte("aaaaaa"),
		[]byte("aaaaab"),
		[]byte("abbbb"),
		[]byte("abbba"),
		[]byte("bbbbba"),
		[]byte("bbbaaa"),
		[]byte("bbbaab"),
		[]byte("aa"),
		[]byte("b"),
	}
	for i := 0; i < len(testCases); i++ {
		c := testCases[i]
		err := n.Add(c, c, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		for j := 0; j < i; j++ {
			d := testCases[j]
			m, err := n.Lookup(d, nil)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !bytes.Equal(m, d) {
				t.Fatalf("expected value %x, got %x", d, m)
			}
		}
	}
}

func TestRemove(t *testing.T) {
	for _, tc := range []struct {
		name     string
		toAdd    [][]byte
		toRemove [][]byte
	}{
		{
			name: "simple",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/1.png"),
				[]byte("img/2.png"),
				[]byte("robots.txt"),
			},
			toRemove: [][]byte{
				[]byte("img/2.png"),
			},
		},
		{
			name: "nested-prefix-is-not-collapsed",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/1.png"),
				[]byte("img/2/test1.png"),
				[]byte("img/2/test2.png"),
				[]byte("robots.txt"),
			},
			toRemove: [][]byte{
				[]byte("img/2/test1.png"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				err := n.Add(c, c, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				for j := 0; j < i; j++ {
					d := tc.toAdd[j]
					m, err := n.Lookup(d, nil)
					if err != nil {
						t.Fatalf("expected no error, got %v", err)
					}
					if !bytes.Equal(m, d) {
						t.Fatalf("expected value %x, got %x", d, m)
					}
				}
			}

			for i := 0; i < len(tc.toRemove); i++ {
				c := tc.toRemove[i]
				err := n.Remove(c, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				_, err = n.Lookup(c, nil)
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("expected not found error, got %v", err)
				}
			}

		})
	}
}