package main

import "testing"

func TestParseBytes(t *testing.T) {
	cases := map[string]int64{
		"":         0,
		"256":      256,
		"256MiB":   256 << 20,
		"256m":     256 << 20,
		"1g":       1 << 30,
		"2GiB":     2 << 30,
		"512kib":   512 << 10,
		"nonsense": 0,
	}
	for in, want := range cases {
		if got := parseBytes(in); got != want {
			t.Errorf("parseBytes(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestSaneRejectsUnlimitedSentinels(t *testing.T) {
	if sane(0) != 0 || sane(-1) != 0 {
		t.Fatal("non-positive limits must be rejected")
	}
	if sane(1 << 40) != 0 { // 1 TiB -> "no container limit"
		t.Fatal("absurdly large limits must be rejected")
	}
	if got := sane(512 << 20); got != 512<<20 {
		t.Fatalf("a real 512 MiB limit should pass through, got %d", got)
	}
}
