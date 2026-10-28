package event

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

// ControlScheme is the scheme of the channels that carry requests to a run.
const ControlScheme = "control"

// ControlChannel is the channel that carries requests of one verb ("stop",
// "compact") for one run: control://<tenant>/<verb>/<hash>. The last segment
// is a hash of the tenant and run, so a run id is never spelled in a channel
// a surface might log.
func ControlChannel(tenant, verb, run string) string {
	h := sha256.New()
	var n [binary.MaxVarintLen64]byte
	for _, f := range []string{tenant, run} {
		h.Write(n[:binary.PutUvarint(n[:], uint64(len(f)))])
		h.Write([]byte(f))
	}
	return ControlScheme + "://" + tenant + "/" + verb + "/" + hex.EncodeToString(h.Sum(nil))[:16]
}

// IsControl reports whether a channel is a control channel.
func IsControl(channel string) bool {
	return strings.HasPrefix(channel, ControlScheme+":")
}

// ChannelHasPrefix reports whether prefix selects channel, segment by
// segment: "slack://T01/C02" selects "slack://T01/C02" and
// "slack://T01/C02?thread=1", and not "slack://T01/C023". A prefix ending in
// "/" selects everything beneath it. An empty prefix selects nothing, and a
// control channel is selected only by a prefix equal to it, so no prefix or
// root ever reaches one by accident.
func ChannelHasPrefix(channel, prefix string) bool {
	if prefix == "" || !strings.HasPrefix(channel, prefix) {
		return false
	}
	if IsControl(channel) {
		return channel == prefix
	}
	if len(channel) == len(prefix) || strings.HasSuffix(prefix, "/") {
		return true
	}
	switch channel[len(prefix)] {
	case '/', '?':
		return true
	}
	return false
}
