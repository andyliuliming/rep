package auctioncellrep

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
)

//go:generate counterfeiter . AuctionCellClient

type AuctionCellClient interface {
	State(logger lager.Logger) (rep.CellState, bool, error)
	Perform(logger lager.Logger, work rep.Work) (rep.Work, error)
	Reset() error
}

var ErrPreloadedRootFSNotFound = errors.New("preloaded rootfs path not found")
var ErrCellUnhealthy = errors.New("internal cell healthcheck failed")
var ErrCellIdMismatch = errors.New("workload cell ID does not match this cell")
var ErrNotEnoughMemory = errors.New("not enough memory for container and additional memory allocation")

type AuctionCellRep struct {
	cellID                string
	repURL                string
	stackPathMap          rep.StackPathMap
	rootFSProviders       rep.RootFSProviders
	stack                 string
	zone                  string
	generateInstanceGuid  func() (string, error)
	client                executor.Client
	evacuationReporter    evacuation_context.EvacuationReporter
	placementTags         []string
	optionalPlacementTags []string
	proxyMemoryAllocation int
	enableContainerProxy  bool
}

func New(
	cellID string,
	repURL string,
	preloadedStackPathMap rep.StackPathMap,
	arbitraryRootFSes []string,
	zone string,
	generateInstanceGuid func() (string, error),
	client executor.Client,
	evacuationReporter evacuation_context.EvacuationReporter,
	placementTags []string,
	optionalPlacementTags []string,
	proxyMemoryAllocation int,
	enableContainerProxy bool,
) *AuctionCellRep {
	return &AuctionCellRep{
		cellID:                cellID,
		repURL:                repURL,
		stackPathMap:          preloadedStackPathMap,
		rootFSProviders:       rootFSProviders(preloadedStackPathMap, arbitraryRootFSes),
		zone:                  zone,
		generateInstanceGuid:  generateInstanceGuid,
		client:                client,
		evacuationReporter:    evacuationReporter,
		placementTags:         placementTags,
		optionalPlacementTags: optionalPlacementTags,
		proxyMemoryAllocation: proxyMemoryAllocation,
		enableContainerProxy:  enableContainerProxy,
	}
}

func rootFSProviders(preloaded rep.StackPathMap, arbitrary []string) rep.RootFSProviders {
	rootFSProviders := rep.RootFSProviders{}
	for _, scheme := range arbitrary {
		rootFSProviders[scheme] = rep.ArbitraryRootFSProvider{}
	}

	stacks := make([]string, 0, len(preloaded))
	for stack, _ := range preloaded {
		stacks = append(stacks, stack)
	}
	rootFSProviders[models.PreloadedRootFSScheme] = rep.NewFixedSetRootFSProvider(stacks...)
	rootFSProviders[models.PreloadedOCIRootFSScheme] = rep.NewFixedSetRootFSProvider(stacks...)

	return rootFSProviders
}

func pathForRootFS(rootFS string, stackPathMap rep.StackPathMap) (string, error) {
	if rootFS == "" {
		return rootFS, nil
	}

	url, err := url.Parse(rootFS)
	if err != nil {
		return "", err
	}

	if url.Scheme == models.PreloadedRootFSScheme {
		path, ok := stackPathMap[url.Opaque]
		if !ok {
			return "", ErrPreloadedRootFSNotFound
		}
		return path, nil
	} else if url.Scheme == models.PreloadedOCIRootFSScheme {
		path, ok := stackPathMap[url.Opaque]
		if !ok {
			return "", ErrPreloadedRootFSNotFound
		}

		return fmt.Sprintf("%s:%s?%s", url.Scheme, path, url.RawQuery), nil
	}

	return rootFS, nil
}

func rootFSURLFromPath(rootfsPath string, stackPathMap rep.StackPathMap) string {
	url, err := url.Parse(rootfsPath)
	if err != nil {
		return rootfsPath
	}

	for k, v := range stackPathMap {
		if rootfsPath == v {
			return fmt.Sprintf("%s:%s", models.PreloadedRootFSScheme, k)
		} else if url.Path == v {
			return fmt.Sprintf("%s:%s?%s", models.PreloadedOCIRootFSScheme, k, url.RawQuery)
		}
	}
	return rootfsPath
}

func (a *AuctionCellRep) State(logger lager.Logger) (rep.CellState, bool, error) {
	logger = logger.Session("auction-state")
	logger.Info("providing")
	logger.Error("################## (andliu) State !!", nil)
	logger.Error("################## (andliu) pre a.client.ListContainers", nil)
	containers, err := a.client.ListContainers(logger)
	if err != nil {
		logger.Error("failed-to-fetch-containers", err)
		return rep.CellState{}, false, err
	}
	logger.Error("################## (andliu) pre a.client.TotalResources", nil)
	totalResources, err := a.client.TotalResources(logger)
	if err != nil {
		logger.Error("failed-to-get-total-resources", err)
		return rep.CellState{}, false, err
	}
	logger.Error("################## (andliu) pre a.client.RemainingResources", nil)
	availableResources, err := a.client.RemainingResources(logger)
	if err != nil {
		logger.Error("failed-to-get-remaining-resource", err)
		return rep.CellState{}, false, err
	}

	logger.Error("################## (andliu) pre a.client.VolumeDrivers", nil)
	volumeDriversG, err := a.client.VolumeDrivers(logger)
	if err != nil {
		logger.Error("failed-to-get-volume-drivers", err)
		return rep.CellState{}, false, err
	}
	logger.Error("################## (andliu) volumeDrivers", nil, lager.Data{"volumedrivers": (volumeDriversG)})

	lrps := []rep.LRP{}
	tasks := []rep.Task{}
	startingContainerCount := 0

	for i := range containers {
		container := &containers[i]

		if containerIsStarting(container) {
			startingContainerCount++
		}

		if container.Tags == nil {
			logger.Error("failed-to-extract-container-tags", nil)
			continue
		}

		placementTagsJSON := container.Tags[rep.PlacementTagsTag]
		var placementTags []string
		err := json.Unmarshal([]byte(placementTagsJSON), &placementTags)
		if err != nil {
			logger.Error("cannot-unmarshal-placement-tags", err, lager.Data{"placement-tags": placementTagsJSON})
		}

		volumeDriversJSON := container.Tags[rep.VolumeDriversTag]
		var volumeDrivers []string
		err = json.Unmarshal([]byte(volumeDriversJSON), &volumeDrivers)
		if err != nil {
			logger.Error("cannot-unmarshal-volume-drivers", err, lager.Data{"volume-drivers": volumeDriversJSON})
		}

		resource := rep.Resource{MemoryMB: int32(container.MemoryMB), DiskMB: int32(container.DiskMB), MaxPids: int32(container.MaxPids)}
		placementConstraint := rep.PlacementConstraint{
			RootFs:        rootFSURLFromPath(container.RootFSPath, a.stackPathMap),
			VolumeDrivers: volumeDrivers,
			PlacementTags: placementTags,
		}

		switch container.Tags[rep.LifecycleTag] {
		case rep.LRPLifecycle:
			key, err := rep.ActualLRPKeyFromTags(container.Tags)
			if err != nil {
				logger.Error("failed-to-extract-key", err)
				continue
			}
			instanceKey, err := rep.ActualLRPInstanceKeyFromContainer(*container, a.cellID)
			if err != nil {
				logger.Error("failed-to-extract-key", err)
				continue
			}
			lrps = append(lrps, rep.NewLRP(instanceKey.InstanceGuid, *key, resource, placementConstraint))
		case rep.TaskLifecycle:
			domain := container.Tags[rep.DomainTag]
			tasks = append(tasks, rep.NewTask(container.Guid, domain, resource, placementConstraint))
		}
	}

	state := rep.NewCellState(
		a.cellID,
		a.repURL,
		a.rootFSProviders,
		a.convertResources(availableResources),
		a.convertResources(totalResources),
		lrps,
		tasks,
		a.zone,
		startingContainerCount,
		a.evacuationReporter.Evacuating(),
		volumeDriversG,
		a.placementTags,
		a.optionalPlacementTags,
	)
	logger.Error("############### (andliu) pre a.client.Healthy", nil)
	healthy := a.client.Healthy(logger)
	if !healthy {
		logger.Error("failed-garden-health-check", nil)
	}

	logger.Info("provided", lager.Data{
		"available-resources": state.AvailableResources,
		"total-resources":     state.TotalResources,
		"num-lrps":            len(state.LRPs),
		"zone":                state.Zone,
		"evacuating":          state.Evacuating,
	})

	return state, healthy, nil
}

func containerIsStarting(container *executor.Container) bool {
	return container.State == executor.StateReserved ||
		container.State == executor.StateInitializing ||
		container.State == executor.StateCreated
}

func (a *AuctionCellRep) Perform(logger lager.Logger, work rep.Work) (rep.Work, error) {
	logger.Error("################# (andliu) Perform", nil)
	var failedWork = rep.Work{}

	logger = logger.Session("auction-work", lager.Data{
		"lrp-starts": len(work.LRPs),
		"tasks":      len(work.Tasks),
		"cell-id":    work.CellID,
	})

	if work.CellID != "" && work.CellID != a.cellID {
		logger.Error("cell-id-mismatch", ErrCellIdMismatch)
		return work, ErrCellIdMismatch
	}

	if a.enableContainerProxy {
		logger.Error("################# (andliu) enableContainerProxy pre a.client.RemainingResources", nil)
		remainingResources, err := a.client.RemainingResources(logger)
		if err != nil {
			logger.Error("failed-gathering-remaining-reosurces", err)
			return work, err
		}
		var totalRequiredMemory = int32(0)

		for _, lrp := range work.LRPs {
			totalRequiredMemory = totalRequiredMemory + lrp.Resource.MemoryMB
			totalRequiredMemory += int32(a.proxyMemoryAllocation)
		}
		if int32(remainingResources.MemoryMB) < totalRequiredMemory {
			logger.Error("not-enough-memory", ErrNotEnoughMemory)
			return work, ErrNotEnoughMemory
		}
	}

	if a.evacuationReporter.Evacuating() {
		return work, nil
	}

	logger.Error("################# (andliu) work is:", nil, lager.Data{"work": work})
	if len(work.LRPs) > 0 {
		lrpLogger := logger.Session("lrp-allocate-instances")
		lrpLogger.Info("################ (andliu) work.LRPs", lager.Data{"lrps": work.LRPs})
		requests, lrpMap, untranslatedLRPs := a.lrpsToAllocationRequest(work.LRPs)
		if len(untranslatedLRPs) > 0 {
			lrpLogger.Info("failed-to-translate-lrps-to-containers", lager.Data{"num-failed-to-translate": len(untranslatedLRPs)})
			failedWork.LRPs = untranslatedLRPs
		}

		lrpLogger.Info("requesting-container-allocation", lager.Data{"num-requesting-allocation": len(requests)})
		lrpLogger.Error("################ (andliu) pre a.client.AllocateContainers", nil, lager.Data{"requests": requests})
		failures, err := a.client.AllocateContainers(logger, requests)
		if err != nil {
			lrpLogger.Error("failed-requesting-container-allocation", err)
			failedWork.LRPs = work.LRPs
		} else {
			lrpLogger.Info("succeeded-requesting-container-allocation", lager.Data{"num-failed-to-allocate": len(failures)})
			for i := range failures {
				failure := &failures[i]
				lrpLogger.Error("container-allocation-failure", failure, lager.Data{"failed-request": &failure.AllocationRequest})
				if lrp, found := lrpMap[failure.Guid]; found {
					failedWork.LRPs = append(failedWork.LRPs, *lrp)
				}
			}
		}
	}

	if len(work.Tasks) > 0 {
		taskLogger := logger.Session("task-allocate-instances")

		taskLogger.Info("################ (andliu) work.Tasks", lager.Data{"tasks": work.Tasks})
		requests, taskMap, failedTasks := a.tasksToAllocationRequests(work.Tasks)
		if len(failedTasks) > 0 {
			taskLogger.Info("failed-to-translate-tasks-to-containers", lager.Data{"num-failed-to-translate": len(failedTasks)})
			failedWork.Tasks = failedTasks
		}

		taskLogger.Info("requesting-container-allocation", lager.Data{"num-requesting-allocation": len(requests)})
		taskLogger.Error("################ (andliu) pre a.client.AllocateContainers", nil)
		failures, err := a.client.AllocateContainers(logger, requests)
		if err != nil {
			taskLogger.Error("failed-requesting-container-allocation", err)
			failedWork.Tasks = work.Tasks
		} else {
			taskLogger.Info("succeeded-requesting-container-allocation", lager.Data{"num-failed-to-allocate": len(failures)})
			for i := range failures {
				failure := &failures[i]
				taskLogger.Error("container-allocation-failure", failure, lager.Data{"failed-request": &failure.AllocationRequest})
				if task, found := taskMap[failure.Guid]; found {
					failedWork.Tasks = append(failedWork.Tasks, *task)
				}
			}
		}
	}

	return failedWork, nil
}

func (a *AuctionCellRep) lrpsToAllocationRequest(lrps []rep.LRP) ([]executor.AllocationRequest, map[string]*rep.LRP, []rep.LRP) {
	requests := make([]executor.AllocationRequest, 0, len(lrps))
	untranslatedLRPs := make([]rep.LRP, 0)
	lrpMap := make(map[string]*rep.LRP, len(lrps))
	for i := range lrps {
		lrp := &lrps[i]
		tags := executor.Tags{}

		instanceGuid, err := a.generateInstanceGuid()
		if err != nil {
			untranslatedLRPs = append(untranslatedLRPs, *lrp)
			continue
		}

		tags[rep.DomainTag] = lrp.Domain
		tags[rep.ProcessGuidTag] = lrp.ProcessGuid
		tags[rep.ProcessIndexTag] = strconv.Itoa(int(lrp.Index))
		tags[rep.LifecycleTag] = rep.LRPLifecycle
		tags[rep.InstanceGuidTag] = instanceGuid

		placementTags, _ := json.Marshal(lrp.PlacementConstraint.PlacementTags)
		volumeDrivers, _ := json.Marshal(lrp.PlacementConstraint.VolumeDrivers)
		tags[rep.PlacementTagsTag] = string(placementTags)
		tags[rep.VolumeDriversTag] = string(volumeDrivers)

		rootFSPath, err := pathForRootFS(lrp.RootFs, a.stackPathMap)
		if err != nil {
			untranslatedLRPs = append(untranslatedLRPs, *lrp)
			continue
		}

		containerGuid := rep.LRPContainerGuid(lrp.ProcessGuid, instanceGuid)
		lrpMap[containerGuid] = lrp

		var resource executor.Resource

		if a.enableContainerProxy {
			resource = executor.NewResource(int(lrp.MemoryMB)+a.proxyMemoryAllocation, int(lrp.DiskMB), int(lrp.MaxPids), rootFSPath)
		} else {
			resource = executor.NewResource(int(lrp.MemoryMB), int(lrp.DiskMB), int(lrp.MaxPids), rootFSPath)
		}
		requests = append(requests, executor.NewAllocationRequest(containerGuid, &resource, tags))
	}

	return requests, lrpMap, untranslatedLRPs
}

func (a *AuctionCellRep) tasksToAllocationRequests(tasks []rep.Task) ([]executor.AllocationRequest, map[string]*rep.Task, []rep.Task) {
	failedTasks := make([]rep.Task, 0)
	taskMap := make(map[string]*rep.Task, len(tasks))
	requests := make([]executor.AllocationRequest, 0, len(tasks))

	for i := range tasks {
		task := &tasks[i]
		taskMap[task.TaskGuid] = task
		rootFSPath, err := pathForRootFS(task.RootFs, a.stackPathMap)
		if err != nil {
			failedTasks = append(failedTasks, *task)
			continue
		}
		tags := executor.Tags{}
		tags[rep.LifecycleTag] = rep.TaskLifecycle
		tags[rep.DomainTag] = task.Domain

		placementTags, _ := json.Marshal(task.PlacementConstraint.PlacementTags)
		volumeDrivers, _ := json.Marshal(task.PlacementConstraint.VolumeDrivers)
		tags[rep.PlacementTagsTag] = string(placementTags)
		tags[rep.VolumeDriversTag] = string(volumeDrivers)

		resource := executor.NewResource(int(task.MemoryMB), int(task.DiskMB), int(task.MaxPids), rootFSPath)
		requests = append(requests, executor.NewAllocationRequest(task.TaskGuid, &resource, tags))
	}

	return requests, taskMap, failedTasks
}

func (a *AuctionCellRep) convertResources(resources executor.ExecutorResources) rep.Resources {
	return rep.Resources{
		MemoryMB:   int32(resources.MemoryMB),
		DiskMB:     int32(resources.DiskMB),
		Containers: resources.Containers,
	}
}

func (a *AuctionCellRep) Reset() error {
	return errors.New("not-a-simulation-rep")
}
