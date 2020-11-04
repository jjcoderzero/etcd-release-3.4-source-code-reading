package raftpb

import (
	"fmt"
	"reflect"
	"sort"
)

// Equivalent returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the differences.
func (cs ConfState) Equivalent(cs2 ConfState) error {
	cs1 := cs
	orig1, orig2 := cs1, cs2
	s := func(sl *[]uint64) {
		*sl = append([]uint64(nil), *sl...)
		sort.Slice(*sl, func(i, j int) bool { return (*sl)[i] < (*sl)[j] })
	}

	for _, cs := range []*ConfState{&cs1, &cs2} {
		s(&cs.Voters)
		s(&cs.Learners)
		s(&cs.VotersOutgoing)
		s(&cs.LearnersNext)
		cs.XXX_unrecognized = nil
	}

	if !reflect.DeepEqual(cs1, cs2) {
		return fmt.Errorf("ConfStates not equivalent after sorting:\n%+#v\n%+#v\nInputs were:\n%+#v\n%+#v", cs1, cs2, orig1, orig2)
	}
	return nil
}
