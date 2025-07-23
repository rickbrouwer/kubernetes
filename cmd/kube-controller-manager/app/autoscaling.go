/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package app implements a server that runs a set of active
// components.  This includes replication controllers, service endpoints and
// nodes.
package app

import (
	"context"
	"reflect"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/controller-manager/controller"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	"k8s.io/kubernetes/pkg/controller/podautoscaler"
	"k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"

	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/custom_metrics"
	"k8s.io/metrics/pkg/client/external_metrics"
)

func newHorizontalPodAutoscalerControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.HorizontalPodAutoscalerController,
		aliases:  []string{"horizontalpodautoscaling"},
		initFunc: startHorizontalPodAutoscalerControllerWithRESTClient,
	}
}

func startHorizontalPodAutoscalerControllerWithRESTClient(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {

	clientConfig := controllerContext.ClientBuilder.ConfigOrDie("horizontal-pod-autoscaler")
	hpaClient := controllerContext.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")

	apiVersionsGetter := custom_metrics.NewAvailableAPIsGetter(hpaClient.Discovery())
	// invalidate the discovery information roughly once per resync interval our API
	// information is *at most* two resync intervals old.
	go custom_metrics.PeriodicallyInvalidate(
		apiVersionsGetter,
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerSyncPeriod.Duration,
		ctx.Done())

	metricsClient := metrics.NewRESTMetricsClient(
		resourceclient.NewForConfigOrDie(clientConfig),
		custom_metrics.NewForConfig(clientConfig, controllerContext.RESTMapper, apiVersionsGetter),
		external_metrics.NewForConfigOrDie(clientConfig),
	)
	return startHPAControllerWithMetricsClient(ctx, controllerContext, metricsClient)
}

func startHPAControllerWithMetricsClient(ctx context.Context, controllerContext ControllerContext, metricsClient metrics.MetricsClient) (controller.Interface, bool, error) {

	hpaClient := controllerContext.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")
	hpaClientConfig := controllerContext.ClientBuilder.ConfigOrDie("horizontal-pod-autoscaler")

	// we don't use cached discovery because DiscoveryScaleKindResolver does its own caching,
	// so we want to re-fetch every time when we actually ask for it
	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(hpaClient.Discovery())
	scaleClient, err := scale.NewForConfig(hpaClientConfig, controllerContext.RESTMapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)
	if err != nil {
		return nil, false, err
	}

	// Create the HPA controller
	hpaController := podautoscaler.NewHorizontalController(
		ctx,
		hpaClient.CoreV1(),
		scaleClient,
		hpaClient.AutoscalingV2(),
		controllerContext.RESTMapper,
		metricsClient,
		controllerContext.InformerFactory.Autoscaling().V2().HorizontalPodAutoscalers(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerSyncPeriod.Duration,
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerDownscaleStabilizationWindow.Duration,
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerTolerance,
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerCPUInitializationPeriod.Duration,
		controllerContext.ComponentConfig.HPAController.HorizontalPodAutoscalerInitialReadinessDelay.Duration,
	)

	// Setup event handlers for per-HPA sync period changes
	hpaInformer := controllerContext.InformerFactory.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
	
	// Add event handlers to detect sync period annotation changes
	hpaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
			if !ok {
				klog.Warningf("Expected HPA object, got %T", obj)
				return
			}
			hpaKey := hpa.Namespace + "/" + hpa.Name
			syncPeriod := hpaController.GetSyncPeriodFromHPA(hpa)
			hpaController.UpdateHPATimer(hpaKey, syncPeriod)
			klog.V(4).Infof("Added HPA %s with sync period %v", hpaKey, syncPeriod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldHPA, ok1 := oldObj.(*autoscalingv2.HorizontalPodAutoscaler)
			newHPA, ok2 := newObj.(*autoscalingv2.HorizontalPodAutoscaler)
			if !ok1 || !ok2 {
				klog.Warningf("Expected HPA objects, got %T and %T", oldObj, newObj)
				return
			}
			
			hpaKey := newHPA.Namespace + "/" + newHPA.Name
			
			// Check if sync period annotation changed
			oldSyncPeriod := ""
			newSyncPeriod := ""
			
			if oldHPA.Annotations != nil {
				oldSyncPeriod = oldHPA.Annotations[podautoscaler.HPASyncPeriodAnnotation]
			}
			if newHPA.Annotations != nil {
				newSyncPeriod = newHPA.Annotations[podautoscaler.HPASyncPeriodAnnotation]
			}
			
			if oldSyncPeriod != newSyncPeriod {
				syncPeriod := hpaController.GetSyncPeriodFromHPA(newHPA)
				hpaController.UpdateHPATimer(hpaKey, syncPeriod)
				klog.V(2).Infof("Sync period changed for HPA %s: '%s' -> '%s' (parsed: %v)", 
					hpaKey, oldSyncPeriod, newSyncPeriod, syncPeriod)
			}
			
			// Also check for other changes that might affect sync behavior
			if !reflect.DeepEqual(oldHPA.Spec, newHPA.Spec) {
				klog.V(4).Infof("HPA spec changed for %s, sync period may need re-evaluation", hpaKey)
			}
		},
		DeleteFunc: func(obj interface{}) {
			hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
			if !ok {
				// Handle DeletedFinalStateUnknown
				if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					hpa, ok = tombstone.Obj.(*autoscalingv2.HorizontalPodAutoscaler)
					if !ok {
						klog.Warningf("Expected HPA object in tombstone, got %T", tombstone.Obj)
						return
					}
				} else {
					klog.Warningf("Expected HPA object, got %T", obj)
					return
				}
			}
			hpaKey := hpa.Namespace + "/" + hpa.Name
			hpaController.CleanupHPATimer(hpaKey)
			klog.V(4).Infof("Deleted HPA %s, cleaned up timer", hpaKey)
		},
	})

	// Start the controller
	go hpaController.Run(ctx, int(controllerContext.ComponentConfig.HPAController.ConcurrentHorizontalPodAutoscalerSyncs))

	klog.V(1).Info("Started HPA controller with per-HPA sync period support")
	return nil, true, nil
}
