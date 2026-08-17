package main

import (
	"context"
	"flag"
	"os"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/restmapper"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:
			scheme,
		MetricsBindAddress: metricsAddr,
		Port:
			9443,
		LeaderElection:
			enableLeaderElection,
		LeaderElectionID:
			"crd-resolver-controller",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&CRDReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Mapper: mgr.GetRESTMapper(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CRD")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// CRDReconciler reconciles a CustomResourceDefinition object
type CRDReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Mapper meta.RESTMapper
	mu     sync.Mutex
}

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile handles CRD events and resets the RESTMapper on deletion
func (r *CRDReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	crd := &apiextensionsv1.CustomResourceDefinition{}
	err := r.Get(ctx, req.NamespacedName, crd)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Info("CRD deleted, resetting RESTMapper", "crd", req.Name)
			r.mu.Lock()
			defer r.mu.Unlock()

			// Safely handle type assertions to check if the mapper implements Reset()
			// or is an instance of *restmapper.DeferredDiscoveryRESTMapper.
			if resettable, ok := r.Mapper.(interface{ Reset() }); ok {
				resettable.Reset()
				log.Info("RESTMapper reset successfully")
			} else if deferredMapper, ok := r.Mapper.(*restmapper.DeferredDiscoveryRESTMapper); ok {
				deferredMapper.Reset()
				log.Info("DeferredDiscoveryRESTMapper reset successfully")
			} else {
				log.Info("RESTMapper does not support Reset")
			}
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CRDReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiextensionsv1.CustomResourceDefinition{}).
		Complete(r)
}
