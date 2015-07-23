package internal_test

import (
	"errors"
	"strconv"

	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context/fake_evacuation_context"
	"github.com/cloudfoundry-incubator/rep/generator/internal"
	"github.com/cloudfoundry-incubator/rep/generator/internal/fake_internal"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/bbserrors"
	fake_legacy_bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	oldmodels "github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("OrdinaryLRPProcessor", func() {
	const expectedCellID = "cell-id"

	var (
		processor          internal.LRPProcessor
		logger             *lagertest.TestLogger
		bbsClient          *fake_bbs.FakeClient
		legacyBBS          *fake_legacy_bbs.FakeRepBBS
		containerDelegate  *fake_internal.FakeContainerDelegate
		evacuationReporter *fake_evacuation_context.FakeEvacuationReporter
	)

	BeforeEach(func() {
		legacyBBS = new(fake_legacy_bbs.FakeRepBBS)
		bbsClient = new(fake_bbs.FakeClient)
		containerDelegate = new(fake_internal.FakeContainerDelegate)
		evacuationReporter = &fake_evacuation_context.FakeEvacuationReporter{}
		evacuationReporter.EvacuatingReturns(false)
		processor = internal.NewLRPProcessor(bbsClient, legacyBBS, containerDelegate, expectedCellID, evacuationReporter, 124)
		logger = lagertest.NewTestLogger("test")
	})

	Describe("Process", func() {
		const sessionPrefix = "test.ordinary-lrp-processor."

		var expectedLrpKey oldmodels.ActualLRPKey
		var expectedInstanceKey oldmodels.ActualLRPInstanceKey
		var expectedNetInfo oldmodels.ActualLRPNetInfo
		var expectedSessionName string

		BeforeEach(func() {
			expectedLrpKey = oldmodels.NewActualLRPKey("process-guid", 2, "domain")
			expectedInstanceKey = oldmodels.NewActualLRPInstanceKey("instance-guid", "cell-id")
			expectedNetInfo = oldmodels.NewActualLRPNetInfo("1.2.3.4", []oldmodels.PortMapping{{ContainerPort: 8080, HostPort: 61999}})
		})

		Context("when given an LRP container", func() {
			var container executor.Container

			BeforeEach(func() {
				container = newLRPContainer(expectedLrpKey, expectedInstanceKey, expectedNetInfo)
			})

			JustBeforeEach(func() {
				processor.Process(logger, container)
			})

			Context("and the container is INVALID", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-invalid-container"
					container.State = executor.StateInvalid
				})

				It("logs an error", func() {
					Expect(logger).To(Say(expectedSessionName))
				})
			})

			Context("and the container is RESERVED", func() {
				BeforeEach(func() {
					expectedSessionName = sessionPrefix + "process-reserved-container"
					container.State = executor.StateReserved
				})

				It("claims the actualLRP in the legacyBBS", func() {
					expectedNewInstanceKey := models.NewActualLRPInstanceKey(expectedInstanceKey.InstanceGuid, expectedInstanceKey.CellID)

					Expect(bbsClient.ClaimActualLRPCallCount()).To(Equal(1))
					processGuid, index, instanceKey := bbsClient.ClaimActualLRPArgsForCall(0)
					Expect(processGuid).To(Equal(expectedLrpKey.ProcessGuid))
					Expect(index).To(Equal(expectedLrpKey.Index))
					Expect(instanceKey).To(Equal(expectedNewInstanceKey))
				})

				Context("when claiming fails because ErrActualLRPCannotBeClaimed", func() {
					BeforeEach(func() {
						bbsClient.ClaimActualLRPReturns(nil, models.ErrActualLRPCannotBeClaimed)
					})

					It("deletes the container", func() {
						Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(1))
						delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
						Expect(containerGuid).To(Equal(container.Guid))
						Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
					})

					It("does not try to run the container", func() {
						Expect(containerDelegate.RunContainerCallCount()).To(Equal(0))
					})
				})

				Context("when claiming fails for an unknown reason", func() {
					BeforeEach(func() {
						bbsClient.ClaimActualLRPReturns(nil, errors.New("boom"))
					})

					It("does not delete the container", func() {
						Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(0))
					})

					It("does not try to run the container", func() {
						Expect(containerDelegate.RunContainerCallCount()).To(Equal(0))
					})
				})

				Context("when claiming succeeds", func() {
					It("runs the container", func() {
						Expect(containerDelegate.RunContainerCallCount()).To(Equal(1))
						delegateLogger, containerGuid := containerDelegate.RunContainerArgsForCall(0)
						Expect(containerGuid).To(Equal(container.Guid))
						Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
					})

					Context("when running fails", func() {
						BeforeEach(func() {
							containerDelegate.RunContainerReturns(false)
						})

						It("removes the actual LRP", func() {
							Expect(bbsClient.RemoveActualLRPCallCount()).To(Equal(1))
							processGuid, index := bbsClient.RemoveActualLRPArgsForCall(0)

							Expect(processGuid).To(Equal(expectedLrpKey.ProcessGuid))
							Expect(index).To(Equal(expectedLrpKey.Index))
						})
					})
				})

				var itClaimsTheLRPOrDeletesTheContainer = func(expectedSessionName string) {
					It("claims the lrp", func() {
						expectedNewInstanceKey := models.NewActualLRPInstanceKey(expectedInstanceKey.InstanceGuid, expectedInstanceKey.CellID)

						Expect(bbsClient.ClaimActualLRPCallCount()).To(Equal(1))
						processGuid, index, instanceKey := bbsClient.ClaimActualLRPArgsForCall(0)
						Expect(processGuid).To(Equal(expectedLrpKey.ProcessGuid))
						Expect(index).To(Equal(expectedLrpKey.Index))
						Expect(instanceKey).To(Equal(expectedNewInstanceKey))
					})

					Context("when the claim fails because ErrActualLRPCannotBeClaimed", func() {
						BeforeEach(func() {
							bbsClient.ClaimActualLRPReturns(nil, models.ErrActualLRPCannotBeClaimed)
						})

						It("deletes the container", func() {
							Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(1))
							delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
							Expect(containerGuid).To(Equal(container.Guid))
							Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
						})
					})

					Context("when the claim fails for an unknown reason", func() {
						BeforeEach(func() {
							bbsClient.ClaimActualLRPReturns(nil, errors.New("boom"))
						})

						It("does not stop or delete the container", func() {
							Expect(containerDelegate.StopContainerCallCount()).To(Equal(0))
							Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(0))
						})
					})
				}

				Context("and the container is INITIALIZING", func() {
					BeforeEach(func() {
						container.State = executor.StateInitializing
					})

					itClaimsTheLRPOrDeletesTheContainer(sessionPrefix + "process-initializing-container")
				})

				Context("and the container is CREATED", func() {
					BeforeEach(func() {
						container.State = executor.StateCreated
					})

					itClaimsTheLRPOrDeletesTheContainer(sessionPrefix + "process-created-container")
				})

				Context("and the container is RUNNING", func() {
					BeforeEach(func() {
						expectedSessionName = sessionPrefix + "process-running-container"
						container.State = executor.StateRunning
						container.ExternalIP = "1.2.3.4"
						container.Ports = []executor.PortMapping{{ContainerPort: 8080, HostPort: 61999}}
					})

					It("starts the lrp in the legacyBBS", func() {
						Expect(legacyBBS.StartActualLRPCallCount()).To(Equal(1))
						legacyBBSLogger, lrpKey, instanceKey, netInfo := legacyBBS.StartActualLRPArgsForCall(0)
						Expect(lrpKey).To(Equal(expectedLrpKey))
						Expect(instanceKey).To(Equal(expectedInstanceKey))
						Expect(netInfo).To(Equal(expectedNetInfo))
						Expect(legacyBBSLogger.SessionName()).To(Equal(expectedSessionName))
					})

					Context("when starting fails because ErrActualLRPCannotBeStarted", func() {
						BeforeEach(func() {
							legacyBBS.StartActualLRPReturns(bbserrors.ErrActualLRPCannotBeStarted)
						})

						It("stops the container", func() {
							Expect(containerDelegate.StopContainerCallCount()).To(Equal(1))
							delegateLogger, containerGuid := containerDelegate.StopContainerArgsForCall(0)
							Expect(containerGuid).To(Equal(container.Guid))
							Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
						})
					})

					Context("when starting fails for an unknown reason", func() {
						BeforeEach(func() {
							legacyBBS.StartActualLRPReturns(errors.New("boom"))
						})

						It("does not stop or delete the container", func() {
							Expect(containerDelegate.StopContainerCallCount()).To(Equal(0))
							Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(0))
						})
					})
				})

				Context("and the container is COMPLETED", func() {
					BeforeEach(func() {
						expectedSessionName = sessionPrefix + "process-completed-container"
						container.State = executor.StateCompleted
					})

					Context("and the container was requested to stop", func() {
						BeforeEach(func() {
							container.RunResult.Stopped = true
						})

						It("removes the actual LRP", func() {
							Expect(bbsClient.RemoveActualLRPCallCount()).To(Equal(1))
							processGuid, index := bbsClient.RemoveActualLRPArgsForCall(0)

							Expect(processGuid).To(Equal(expectedLrpKey.ProcessGuid))
							Expect(index).To(Equal(expectedLrpKey.Index))
						})

						Context("when the removal succeeds", func() {
							It("deletes the container", func() {
								Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(1))
								delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
								Expect(containerGuid).To(Equal(container.Guid))
								Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
							})
						})

						Context("when the removal fails", func() {
							BeforeEach(func() {
								bbsClient.RemoveActualLRPReturns(errors.New("whoops"))
							})

							It("deletes the container", func() {
								Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(1))
								delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
								Expect(containerGuid).To(Equal(container.Guid))
								Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
							})
						})
					})

					Context("and the container was not requested to stop", func() {
						BeforeEach(func() {
							container.RunResult.Stopped = false
							container.RunResult.FailureReason = "crashed"
						})

						It("crashes the actual LRP", func() {
							Expect(legacyBBS.CrashActualLRPCallCount()).To(Equal(1))
							legacyBBSLogger, lrpKey, instanceKey, reason := legacyBBS.CrashActualLRPArgsForCall(0)
							Expect(lrpKey).To(Equal(expectedLrpKey))
							Expect(instanceKey).To(Equal(expectedInstanceKey))
							Expect(reason).To(Equal("crashed"))
							Expect(legacyBBSLogger.SessionName()).To(Equal(expectedSessionName))
						})

						It("deletes the container", func() {
							Expect(containerDelegate.DeleteContainerCallCount()).To(Equal(1))
							delegateLogger, containerGuid := containerDelegate.DeleteContainerArgsForCall(0)
							Expect(containerGuid).To(Equal(container.Guid))
							Expect(delegateLogger.SessionName()).To(Equal(expectedSessionName))
						})
					})
				})

				Context("and the container is in an invalid state", func() {
					BeforeEach(func() {
						container.State = executor.StateInvalid
					})

					It("logs the container as a warning", func() {
						Expect(logger).To(Say(sessionPrefix + "process-invalid-container.not-processing-container-in-invalid-state"))
					})
				})
			})
		})
	})
})

func newLRPContainer(lrpKey oldmodels.ActualLRPKey, instanceKey oldmodels.ActualLRPInstanceKey, netInfo oldmodels.ActualLRPNetInfo) executor.Container {
	ports := []executor.PortMapping{}
	for _, portMap := range netInfo.Ports {
		ports = append(ports, executor.PortMapping{
			ContainerPort: portMap.ContainerPort,
			HostPort:      portMap.HostPort,
		})
	}

	return executor.Container{
		Guid:       rep.LRPContainerGuid(lrpKey.ProcessGuid, instanceKey.InstanceGuid),
		Action:     &oldmodels.RunAction{Path: "true"},
		ExternalIP: netInfo.Address,
		Ports:      ports,
		Tags: executor.Tags{
			rep.ProcessGuidTag:  lrpKey.ProcessGuid,
			rep.InstanceGuidTag: instanceKey.InstanceGuid,
			rep.ProcessIndexTag: strconv.Itoa(lrpKey.Index),
			rep.DomainTag:       lrpKey.Domain,
		},
	}
}
