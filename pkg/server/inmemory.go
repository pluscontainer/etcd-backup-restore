// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	etcdclient "github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/member"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"sigs.k8s.io/yaml"
)

// serveLearnerJoinConfig serves the etcd config with the current cluster members as initial cluster and state `existing`
// if the member was added as learner. It reports whether it handled the request.
func (h *HTTPHandler) serveLearnerJoinConfig(rw http.ResponseWriter, req *http.Request, config map[string]interface{}, outputFileName string) bool {
	if !miscellaneous.IsLearnerJoinPending(fmt.Sprint(config["data-dir"])) {
		return false
	}

	name := fmt.Sprint(config["name"])
	peerURLs := strings.Split(fmt.Sprint(config["initial-advertise-peer-urls"]), ",")
	initialCluster, err := learnerJoinInitialCluster(req.Context(), *h.EtcdConnectionConfig, name, peerURLs)
	if err != nil {
		h.Logger.Warnf("Unable to determine the cluster to join as learner: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return true
	}
	config["initial-cluster"] = initialCluster
	config["initial-cluster-state"] = miscellaneous.ClusterStateExisting

	data, err := yaml.Marshal(&config)
	if err != nil {
		h.Logger.Warnf("Unable to marshal data to etcd config yaml file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return true
	}
	if err := os.WriteFile(outputFileName, data, 0600); err != nil {
		h.Logger.Warnf("Unable to write etcd config file: %v", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return true
	}
	http.ServeFile(rw, req, outputFileName)

	h.Logger.Infof("Served config for ETCD instance joining as learner with initial cluster %s.", initialCluster)
	return true
}

// learnerJoinInitialCluster returns the initial-cluster value for the member joining the running cluster.
func learnerJoinInitialCluster(ctx context.Context, etcdConn brtypes.EtcdConnectionConfig, name string, peerURLs []string) (string, error) {
	clientFactory := etcdutil.NewFactory(etcdConn, etcdclient.UseServiceEndpoints(true))
	cli, err := clientFactory.NewCluster()
	if err != nil {
		return "", fmt.Errorf("failed to build etcd cluster client: %w", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(ctx, member.EtcdTimeout)
	defer cancel()
	members, err := cli.MemberList(ctx)
	if err != nil {
		return "", fmt.Errorf("could not list members: %w", err)
	}
	return miscellaneous.JoinInitialCluster(members.Members, name, peerURLs)
}
