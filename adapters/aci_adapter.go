package adapters

import (
	"io"

	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager"
	"github.com/virtualcloudfoundry/goaci/aci"
)

// make this run in locally for now. expose one api in the future.

type client struct {
	inner executor.Client
}

func NewAciAdapterClient(toAdapt executor.Client) executor.Client {
	return &client{
		inner: toAdapt,
	}
}

func (c *client) AllocateContainers(logger lager.Logger, requests []executor.AllocationRequest) ([]executor.AllocationFailure, error) {
	aciClient, err := aci.NewClient()
	if err != nil {
		logger.Error("############### (andliu) AllocateContainers. aci error", nil, lager.Data{"err": err})
	}
	if aciClient != nil {

	}
	// aciClient.CreateContainerGroup()
	failures := make([]executor.AllocationFailure, 0)
	logger.Error("############### (andliu) virtualallocationcontainers", nil, lager.Data{"failure": failures})

	return c.inner.AllocateContainers(logger, requests)
}

func (c *client) GetContainer(logger lager.Logger, guid string) (executor.Container, error) {
	return c.inner.GetContainer(logger, guid)
}

func (c *client) RunContainer(logger lager.Logger, request *executor.RunRequest) error {
	logger.Error("################# (andliu) RunContainer in aci adapter", nil,
		lager.Data{
			"request": request,
		})
	lifecycle := request.Tags["lifecycle"]
	logger.Error("################# (andliu) lifecycle in aci adapter", nil, lager.Data{"lifecycle": lifecycle})

	return c.inner.RunContainer(logger, request)
}

func (c *client) ListContainers(logger lager.Logger) ([]executor.Container, error) {
	return c.inner.ListContainers(logger)
}

func (c *client) GetBulkMetrics(logger lager.Logger) (map[string]executor.Metrics, error) {
	return c.inner.GetBulkMetrics(logger)
}

func (c *client) StopContainer(logger lager.Logger, guid string) error {
	return c.inner.StopContainer(logger, guid)
}

func (c *client) DeleteContainer(logger lager.Logger, guid string) error {
	return c.inner.DeleteContainer(logger, guid)
}

func (c *client) RemainingResources(logger lager.Logger) (executor.ExecutorResources, error) {
	return c.inner.RemainingResources(logger)
}

func (c *client) Ping(logger lager.Logger) error {
	return c.inner.Ping(logger)
}

func (c *client) TotalResources(logger lager.Logger) (executor.ExecutorResources, error) {
	return c.inner.TotalResources(logger)
}

func (c *client) GetFiles(logger lager.Logger, guid, sourcePath string) (io.ReadCloser, error) {
	return c.inner.GetFiles(logger, guid, sourcePath)
}

func (c *client) VolumeDrivers(logger lager.Logger) ([]string, error) {
	return c.inner.VolumeDrivers(logger)
}

func (c *client) SubscribeToEvents(logger lager.Logger) (executor.EventSource, error) {
	return c.inner.SubscribeToEvents(logger)
}

func (c *client) Healthy(logger lager.Logger) bool {
	return c.inner.Healthy(logger)
}

func (c *client) SetHealthy(logger lager.Logger, healthy bool) {
	c.inner.SetHealthy(logger, healthy)
}

func (c *client) Cleanup(logger lager.Logger) {

}
