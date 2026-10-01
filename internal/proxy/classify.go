package proxy

import "strings"

// Class is what a connection belongs to. Faults may target only the system under
// test; Crashpoint must never fault its own harness traffic, or it would corrupt
// the ground truth it checks against (DESIGN.md §7.2).
type Class uint8

const (
	// ClassUnknown is the zero value: no request seen yet, so the client id is
	// not known. Unknown connections are NOT faultable — fail safe.
	ClassUnknown Class = iota
	ClassSUT
	ClassHarness
)

func (c Class) String() string {
	switch c {
	case ClassSUT:
		return "sut"
	case ClassHarness:
		return "harness"
	default:
		return "unknown"
	}
}

// Classifier decides a connection's class from the client id in its first
// request.
type Classifier func(clientID string) Class

// DefaultHarnessPrefix is the client-id prefix Crashpoint's own clients use (the
// workload sets "crashpoint-workload"). Everything else is the SUT.
const DefaultHarnessPrefix = "crashpoint-"

// PrefixClassifier classifies a connection as harness when its client id starts
// with harnessPrefix, otherwise as the SUT.
func PrefixClassifier(harnessPrefix string) Classifier {
	return func(clientID string) Class {
		if strings.HasPrefix(clientID, harnessPrefix) {
			return ClassHarness
		}
		return ClassSUT
	}
}
