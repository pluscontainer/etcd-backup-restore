// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package miscellaneous

import (
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

var _ = Describe("In-memory etcd", func() {
	const (
		url0 = "https://etcd-main-0.etcd-main-peer.ns.svc:2380"
		url1 = "https://etcd-main-1.etcd-main-peer.ns.svc:2380"
		url2 = "https://etcd-main-2.etcd-main-peer.ns.svc:2380"
	)

	Describe("learner join marker", func() {
		It("should be pending only between mark and clear", func() {
			dataDir := filepath.Join(GinkgoT().TempDir(), "new.etcd")

			Expect(IsLearnerJoinPending(dataDir)).To(BeFalse())
			Expect(MarkLearnerJoin(dataDir)).To(Succeed())
			Expect(IsLearnerJoinPending(dataDir)).To(BeTrue())
			Expect(ClearLearnerJoin(dataDir)).To(Succeed())
			Expect(IsLearnerJoinPending(dataDir)).To(BeFalse())
			Expect(ClearLearnerJoin(dataDir)).To(Succeed())
		})

		It("should not be removed together with the data directory", func() {
			dataDir := filepath.Join(GinkgoT().TempDir(), "new.etcd")
			Expect(os.MkdirAll(dataDir, 0700)).To(Succeed())
			Expect(MarkLearnerJoin(dataDir)).To(Succeed())

			Expect(os.RemoveAll(dataDir)).To(Succeed())
			Expect(IsLearnerJoinPending(dataDir)).To(BeTrue())
		})
	})

	Describe("GetMemberAndPeerNames", func() {
		var configFile string

		writeConfig := func(podName string, config map[string]interface{}) {
			data, err := yaml.Marshal(config)
			Expect(err).ToNot(HaveOccurred())
			Expect(os.WriteFile(configFile, data, 0600)).To(Succeed())
			GinkgoT().Setenv("POD_NAME", podName)
		}

		BeforeEach(func() {
			configFile = filepath.Join(GinkgoT().TempDir(), "etcd.conf.yaml")
		})

		It("should return this member and the other members of the initial cluster", func() {
			writeConfig("etcd-main-1", map[string]interface{}{
				"initial-cluster": "etcd-main-0=" + url0 + ",etcd-main-1=" + url1 + ",etcd-main-2=" + url2,
				"initial-advertise-peer-urls": map[string][]string{
					"etcd-main-0": {url0},
					"etcd-main-1": {url1},
					"etcd-main-2": {url2},
				},
			})

			name, peers, err := GetMemberAndPeerNames(configFile)
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("etcd-main-1"))
			Expect(peers).To(Equal([]string{"etcd-main-0", "etcd-main-2"}))
		})

		It("should return no peers for a single member", func() {
			writeConfig("etcd-main-0", map[string]interface{}{
				"initial-cluster":             "etcd-main-0=" + url0,
				"initial-advertise-peer-urls": map[string][]string{"etcd-main-0": {url0}},
			})

			name, peers, err := GetMemberAndPeerNames(configFile)
			Expect(err).ToNot(HaveOccurred())
			Expect(name).To(Equal("etcd-main-0"))
			Expect(peers).To(BeEmpty())
		})

		It("should fail if no member of the initial cluster has this member's peer URLs", func() {
			writeConfig("etcd-main-0", map[string]interface{}{
				"initial-cluster":             "etcd-main-1=" + url1,
				"initial-advertise-peer-urls": map[string][]string{"etcd-main-0": {url0}},
			})

			_, _, err := GetMemberAndPeerNames(configFile)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("JoinInitialCluster", func() {
		It("should name this member by its peer URLs and keep the names of the others", func() {
			members := []*etcdserverpb.Member{
				{ID: 0x1, Name: "etcd-main-0", PeerURLs: []string{url0}},
				{ID: 0x2, Name: "", PeerURLs: []string{url1}, IsLearner: true},
			}

			initialCluster, err := JoinInitialCluster(members, "etcd-main-1", []string{url1})
			Expect(err).ToNot(HaveOccurred())
			Expect(initialCluster).To(Equal("etcd-main-0=" + url0 + ",etcd-main-1=" + url1))
		})

		It("should name unstarted other members by their ID", func() {
			members := []*etcdserverpb.Member{
				{ID: 0x1, Name: "etcd-main-0", PeerURLs: []string{url0}},
				{ID: 0xab, Name: "", PeerURLs: []string{url2}},
				{ID: 0x2, Name: "", PeerURLs: []string{url1}, IsLearner: true},
			}

			initialCluster, err := JoinInitialCluster(members, "etcd-main-1", []string{url1})
			Expect(err).ToNot(HaveOccurred())
			Expect(initialCluster).To(Equal("etcd-main-0=" + url0 + ",ab=" + url2 + ",etcd-main-1=" + url1))
		})

		It("should fail if this member is not part of the cluster", func() {
			members := []*etcdserverpb.Member{{ID: 0x1, Name: "etcd-main-0", PeerURLs: []string{url0}}}

			_, err := JoinInitialCluster(members, "etcd-main-1", []string{url1})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("IsRenewedByCurrentPod", func() {
		now := time.Date(2026, 10, 20, 4, 0, 0, 0, time.UTC)
		lease := func(holder *string, renewed *time.Time) *coordinationv1.Lease {
			l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "etcd-main-1"}}
			l.Spec.HolderIdentity = holder
			if renewed != nil {
				l.Spec.RenewTime = &metav1.MicroTime{Time: *renewed}
			}
			return l
		}
		podCreated := now.Add(-time.Hour)

		DescribeTable("should report whether the current pod renewed the lease recently",
			func(l *coordinationv1.Lease, podCreated time.Time, expected bool) {
				Expect(IsRenewedByCurrentPod(l, podCreated, now, time.Minute)).To(Equal(expected))
			},
			Entry("renewed recently by the current pod", lease(ptr.To("a:b:Member"), ptr.To(now.Add(-10*time.Second))), podCreated, true),
			Entry("renewed long ago", lease(ptr.To("a:b:Member"), ptr.To(now.Add(-10*time.Hour))), podCreated, false),
			Entry("renewed recently by a previous pod", lease(ptr.To("a:b:Member"), ptr.To(now.Add(-30*time.Second))), now.Add(-5*time.Second), false),
			Entry("without holder", lease(nil, ptr.To(now)), podCreated, false),
			Entry("never renewed", lease(ptr.To("a:b:Member"), nil), podCreated, false),
		)
	})

	Describe("PeerPodName", func() {
		It("should map member names to pod names", func() {
			Expect(PeerPodName("etcd-main-0", "etcd-main-2")).To(Equal("etcd-main-2"))
			Expect(PeerPodName("etcd-main-0", "seed01-etcd-main-1")).To(Equal("etcd-main-1"))
		})
	})
})
