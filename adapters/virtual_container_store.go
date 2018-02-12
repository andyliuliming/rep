package adapters

import (
	"io"
	"sync"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/executor"
	containerstore "code.cloudfoundry.org/executor/depot/containerstore"
	"code.cloudfoundry.org/lager"
	"github.com/tedsuo/ifrit"
)

// mock /Users/andy/workspace/diego-release/src/code.cloudfoundry.org/executor/depot/containerstore/containerstore.go for now

type virtualContainerStore struct {
	containerConfig containerstore.ContainerConfig
	// containers      *nodeMap
	clock       clock.Clock
	reapingLock *sync.RWMutex
}

func NewVirtualContainerStore(
	totalCapacity *executor.ExecutorResources,
) containerstore.ContainerStore {
	return &virtualContainerStore{}
}

func (cs *virtualContainerStore) Reserve(logger lager.Logger, req *executor.AllocationRequest) (executor.Container, error) {
	return executor.Container{}, nil
}

// Reserve(logger lager.Logger, req *executor.AllocationRequest) (executor.Container, error)
func (cs *virtualContainerStore) Destroy(logger lager.Logger, guid string) error {
	return nil
}

// Container Operations
func (cs *virtualContainerStore) Initialize(logger lager.Logger, req *executor.RunRequest) error {
	return nil
}
func (cs *virtualContainerStore) Create(logger lager.Logger, guid string) (executor.Container, error) {
	return executor.Container{}, nil
}
func (cs *virtualContainerStore) Run(logger lager.Logger, guid string) error {
	return nil
}
func (cs *virtualContainerStore) Stop(logger lager.Logger, guid string) error {
	return nil
}

// Getters
func (cs *virtualContainerStore) Get(logger lager.Logger, guid string) (executor.Container, error) {
	return executor.Container{}, nil
}
func (cs *virtualContainerStore) List(logger lager.Logger) []executor.Container {
	containers := make([]executor.Container, 0)
	return containers
}
func (cs *virtualContainerStore) Metrics(logger lager.Logger) (map[string]executor.ContainerMetrics, error) {
	containerMetrics := map[string]executor.ContainerMetrics{}
	return containerMetrics, nil
}
func (cs *virtualContainerStore) RemainingResources(logger lager.Logger) executor.ExecutorResources {
	return executor.ExecutorResources{}
}
func (cs *virtualContainerStore) GetFiles(logger lager.Logger, guid, sourcePath string) (io.ReadCloser, error) {
	return nil, nil
}

// Cleanup
func (cs *virtualContainerStore) NewRegistryPruner(logger lager.Logger) ifrit.Runner {
	return containerstore.TempNewRegistryPruner(logger, &cs.containerConfig, cs.clock, nil)
}
func (cs *virtualContainerStore) NewContainerReaper(logger lager.Logger) ifrit.Runner {
	return containerstore.TempNewContainerReaper(logger, &cs.containerConfig, cs.clock, nil, nil, cs.reapingLock)
}

// shutdown the dependency manager
func (cs *virtualContainerStore) Cleanup(logger lager.Logger) {

}
