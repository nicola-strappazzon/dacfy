package human_test

import (
	"testing"
	"time"

	"github.com/nicola-strappazzon/dacfy/human"

	"github.com/stretchr/testify/assert"
)

func TestDuration(t *testing.T) {
	cases := []struct {
		name   string
		in     time.Duration
		expect string
	}{
		{"zero", 0, "00:00:00"},
		{"seconds exact", 3 * time.Second, "00:00:03"},
		{"rounds down .4", 3400 * time.Millisecond, "00:00:03"},
		{"rounds up .5", 3500 * time.Millisecond, "00:00:04"},
		{"hms mixed", 1*time.Hour + 2*time.Minute + 3*time.Second, "01:02:03"},
		{"over 24h", 25 * time.Hour, "25:00:00"},
		{"complex", 12*time.Hour + 34*time.Minute + 56*time.Second + 789*time.Millisecond, "12:34:57"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := human.Duration(tc.in)
			assert.Equal(t, tc.expect, got)
		})
	}
}

func TestBytes(t *testing.T) {
	cases := []struct {
		name   string
		in     uint64
		expect string
	}{
		{"zero", 0, "0"},
		{"one byte", 1, "1 bytes"},
		{"hundreds", 999, "999 bytes"},
		{"just below KiB", 1023, "1023 bytes"},
		{"one KiB", 1024, "1 KiB"},
		{"one and half KiB exact", 1536, "1.50 KiB"},
		{"one point five zero KiB", 1537, "1.50 KiB"},
		{"one MiB", 1024 * 1024, "1 MiB"},
		{"one and quarter MiB", 1024*1024 + 256*1024, "1.25 MiB"},
		{"one GiB", 1024 * 1024 * 1024, "1 GiB"},
		{"one and three quarters GiB", uint64(1.75 * float64(1024*1024*1024)), "1.75 GiB"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := human.Bytes(tc.in)
			assert.Equal(t, tc.expect, got)
		})
	}
}
