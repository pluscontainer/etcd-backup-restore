// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package initializer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/types"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// peerLeaseMaxAge is the maximum age of a member lease renewal for the member to count as running.
const peerLeaseMaxAge = time.Minute

// beginInMemoryInitialization clears the learner join marker and reports whether this member restores the cluster from the backup.
func (e *EtcdInitializer) beginInMemoryInitialization(ctx context.Context, logger *logrus.Entry) (bool, error) {
	if err := miscellaneous.ClearLearnerJoin(e.Config.RestoreOptions.Config.DataDir); err != nil {
		return false, fmt.Errorf("failed to remove learner join marker: %w", err)
	}
	return restoresFromBackup(ctx, logger)
}

// restoresFromBackup reports whether this member is the first member and no other member's current pod renewed its
// lease within peerLeaseMaxAge.
func restoresFromBackup(ctx context.Context, logger *logrus.Entry) (bool, error) {
	podName, namespace := os.Getenv("POD_NAME"), os.Getenv("POD_NAMESPACE")
	if !strings.HasSuffix(podName, "-0") {
		return false, nil
	}
	_, peers, err := miscellaneous.GetMemberAndPeerNames(miscellaneous.GetConfigFilePath())
	if err != nil {
		return false, fmt.Errorf("failed to read cluster members from etcd config: %w", err)
	}
	if len(peers) == 0 {
		return true, nil
	}

	cl, err := miscellaneous.GetKubernetesClientSetOrError()
	if err != nil {
		return false, fmt.Errorf("failed to create clientset: %w", err)
	}
	now := time.Now()
	var running []string
	for _, name := range peers {
		lease := &coordinationv1.Lease{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, lease); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("failed to get member lease %s: %w", name, err)
		}
		pod := &corev1.Pod{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: miscellaneous.PeerPodName(podName, name)}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("failed to get pod of member %s: %w", name, err)
		}
		if miscellaneous.IsRenewedByCurrentPod(lease, pod.CreationTimestamp.Time, now, peerLeaseMaxAge) {
			running = append(running, name)
		}
	}

	if len(running) > 0 {
		logger.Infof("Members %v are running, joining the cluster instead of restoring from the backup", running)
		return false, nil
	}
	logger.Info("No other member is running, restoring from the backup")
	return true, nil
}

// setRestoredMemberIdentity sets name, peer URLs, initial cluster and cluster token of the restored member to the values of this member.
func setRestoredMemberIdentity(ro *brtypes.RestoreOptions, logger *logrus.Logger) {
	configFile := miscellaneous.GetConfigFilePath()
	name, _, err := miscellaneous.GetMemberAndPeerNames(configFile)
	if err != nil {
		logger.Warnf("Restoring with default member identity: %v", err)
		return
	}
	peerURLs, err := miscellaneous.GetMemberPeerURLs(configFile)
	if err != nil {
		logger.Warnf("Restoring with default member identity: %v", err)
		return
	}
	urls, err := types.NewURLs(peerURLs)
	if err != nil {
		logger.Warnf("Restoring with default member identity: %v", err)
		return
	}
	config, err := miscellaneous.ReadConfigFileAsMap(configFile)
	if err != nil {
		logger.Warnf("Restoring with default member identity: %v", err)
		return
	}

	ro.Config.Name = name
	ro.Config.InitialAdvertisePeerURLs = peerURLs
	ro.Config.InitialCluster = types.URLsMap{name: urls}.String()
	ro.PeerURLs = urls
	if token, ok := config["initial-cluster-token"].(string); ok && token != "" {
		ro.Config.InitialClusterToken = token
	}
	logger.Infof("Restoring as member %s with peer URLs %v", name, peerURLs)
}
