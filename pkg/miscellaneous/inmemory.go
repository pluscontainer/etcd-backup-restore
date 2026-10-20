// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package miscellaneous

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/types"
	coordinationv1 "k8s.io/api/coordination/v1"
)

// LearnerJoinMarker returns the path of the file marking that the member owning dataDir was added to the cluster as learner.
func LearnerJoinMarker(dataDir string) string {
	return filepath.Clean(dataDir) + ".learner"
}

// MarkLearnerJoin creates the learner join marker for dataDir.
func MarkLearnerJoin(dataDir string) error {
	return os.WriteFile(LearnerJoinMarker(dataDir), nil, 0600)
}

// ClearLearnerJoin removes the learner join marker for dataDir.
func ClearLearnerJoin(dataDir string) error {
	if err := os.Remove(LearnerJoinMarker(dataDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// IsLearnerJoinPending reports whether the learner join marker for dataDir exists.
func IsLearnerJoinPending(dataDir string) bool {
	_, err := os.Stat(LearnerJoinMarker(dataDir))
	return err == nil
}

// GetMemberAndPeerNames returns the name of this member and the names of all other members of the initial cluster in the etcd config file.
func GetMemberAndPeerNames(configFile string) (string, []string, error) {
	config, err := ReadConfigFileAsMap(configFile)
	if err != nil {
		return "", nil, err
	}
	peerURLs, err := GetMemberPeerURLs(configFile)
	if err != nil {
		return "", nil, err
	}
	cluster, err := types.NewURLsMap(fmt.Sprint(config["initial-cluster"]))
	if err != nil {
		return "", nil, fmt.Errorf("invalid initial-cluster: %w", err)
	}

	var name string
	var peers []string
	for n, urls := range cluster {
		if equalURLs(urls.StringSlice(), peerURLs) {
			name = n
		} else {
			peers = append(peers, n)
		}
	}
	if name == "" {
		return "", nil, fmt.Errorf("no member with peer URLs %v in initial-cluster", peerURLs)
	}
	slices.Sort(peers)
	return name, peers, nil
}

// JoinInitialCluster returns the initial-cluster value for the member with the given name and peer URLs joining a cluster with the given members.
func JoinInitialCluster(members []*etcdserverpb.Member, name string, peerURLs []string) (string, error) {
	var entries []string
	found := false
	for _, m := range members {
		memberName := m.Name
		if equalURLs(m.PeerURLs, peerURLs) {
			memberName = name
			found = true
		} else if memberName == "" {
			memberName = strconv.FormatUint(m.ID, 16)
		}
		for _, u := range m.PeerURLs {
			entries = append(entries, memberName+"="+u)
		}
	}
	if !found {
		return "", fmt.Errorf("member %s with peer URLs %v is not part of the cluster", name, peerURLs)
	}
	return strings.Join(entries, ","), nil
}

// IsRenewedByCurrentPod reports whether the lease was renewed within maxAge before now and not before the pod
// holding the member was created.
func IsRenewedByCurrentPod(lease *coordinationv1.Lease, podCreated, now time.Time, maxAge time.Duration) bool {
	if lease.Spec.HolderIdentity == nil || lease.Spec.RenewTime == nil {
		return false
	}
	renewed := lease.Spec.RenewTime.Time
	return now.Sub(renewed) < maxAge && !renewed.Before(podCreated)
}

// PeerPodName returns the name of the pod of the member peerName, given the name of this member's pod.
func PeerPodName(podName, peerName string) string {
	return podName[:strings.LastIndex(podName, "-")] + peerName[strings.LastIndex(peerName, "-"):]
}

func equalURLs(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}
