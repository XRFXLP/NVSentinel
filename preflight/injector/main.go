package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/NVIDIA/NVSentinel/preflight/pkg/gang"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func main() {
	var (
		port            int
		certDir         string
		certName        string
		keyName         string
		gangDiscoverers string

		// Preflight configuration
		preflightImage          string
		dcgmHostengineAddr      string
		dcgmDiagLevel           string
		platformConnectorSocket string
		gpuResourceNames        string

		// NCCL All-Reduce configuration
		ncclEnabled       bool
		ncclImage         string
		ncclBWThreshold   string
		ncclNProcsPerNode string
		ncclMasterPort    string
	)

	// Set up zap logger for controller-runtime
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)

	// Server flags
	flag.IntVar(&port, "port", 8443, "Webhook server port")
	flag.StringVar(&certDir, "cert-dir", "/certs", "Directory containing TLS certs")
	flag.StringVar(&certName, "cert-name", "tls.crt", "TLS certificate filename")
	flag.StringVar(&keyName, "key-name", "tls.key", "TLS key filename")
	flag.StringVar(&gangDiscoverers, "gang-discoverers", "volcano,workloadref",
		"Comma-separated list of gang discoverers to use in priority order (volcano, workloadref)")

	// Preflight configuration flags
	flag.StringVar(&preflightImage, "preflight-image", "",
		"Container image for preflight checker (default: ghcr.io/nvidia/nvsentinel/preflight-dcgm-diag:latest)")
	flag.StringVar(&dcgmHostengineAddr, "dcgm-hostengine-addr", "",
		"DCGM hostengine address (default: dcgm-hostengine.nvsentinel.svc:5555)")
	flag.StringVar(&dcgmDiagLevel, "dcgm-diag-level", "",
		"DCGM diagnostic level: 1=quick, 2=extended, 3=full (default: 2)")
	flag.StringVar(&platformConnectorSocket, "platform-connector-socket", "",
		"Platform Connector Unix socket path (default: /var/run/nvsentinel/nvsentinel.sock)")
	flag.StringVar(&gpuResourceNames, "gpu-resource-names", "",
		"Comma-separated list of GPU resource names to detect (default: nvidia.com/gpu)")

	// NCCL All-Reduce flags
	flag.BoolVar(&ncclEnabled, "nccl-enabled", false,
		"Enable NCCL all-reduce preflight check for multi-node gang jobs")
	flag.StringVar(&ncclImage, "nccl-image", "",
		"Container image for NCCL all-reduce checker (default: ghcr.io/nvidia/nvsentinel/preflight-nccl-allreduce:latest)")
	flag.StringVar(&ncclBWThreshold, "nccl-bw-threshold", "",
		"Minimum bus bandwidth threshold in GB/s (default: 100)")
	flag.StringVar(&ncclNProcsPerNode, "nccl-nprocs-per-node", "",
		"Number of GPUs per node for NCCL (default: 8)")
	flag.StringVar(&ncclMasterPort, "nccl-master-port", "",
		"Port for PyTorch distributed rendezvous (default: 29500)")

	flag.Parse()

	// Initialize the logger
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("preflight-injector")

	// Allow environment variable overrides
	if envImage := os.Getenv("PREFLIGHT_IMAGE"); envImage != "" && preflightImage == "" {
		preflightImage = envImage
	}
	if envAddr := os.Getenv("DCGM_HOSTENGINE_ADDR"); envAddr != "" && dcgmHostengineAddr == "" {
		dcgmHostengineAddr = envAddr
	}
	if envLevel := os.Getenv("DCGM_DIAG_LEVEL"); envLevel != "" && dcgmDiagLevel == "" {
		dcgmDiagLevel = envLevel
	}
	if envSocket := os.Getenv("PLATFORM_CONNECTOR_SOCKET"); envSocket != "" && platformConnectorSocket == "" {
		platformConnectorSocket = envSocket
	}
	if envGPU := os.Getenv("GPU_RESOURCE_NAMES"); envGPU != "" && gpuResourceNames == "" {
		gpuResourceNames = envGPU
	}

	// NCCL environment variable overrides
	if os.Getenv("NCCL_ENABLED") == "true" {
		ncclEnabled = true
	}
	if envNCCLImage := os.Getenv("NCCL_IMAGE"); envNCCLImage != "" && ncclImage == "" {
		ncclImage = envNCCLImage
	}
	if envBW := os.Getenv("NCCL_BW_THRESHOLD"); envBW != "" && ncclBWThreshold == "" {
		ncclBWThreshold = envBW
	}
	if envProcs := os.Getenv("NCCL_NPROCS_PER_NODE"); envProcs != "" && ncclNProcsPerNode == "" {
		ncclNProcsPerNode = envProcs
	}
	if envPort := os.Getenv("NCCL_MASTER_PORT"); envPort != "" && ncclMasterPort == "" {
		ncclMasterPort = envPort
	}

	// Set up the scheme
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		log.Error(err, "Failed to add corev1 to scheme")
		os.Exit(1)
	}

	// Create Kubernetes client for gang discovery
	cfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error(err, "Failed to get kubeconfig")
		os.Exit(1)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		log.Error(err, "Failed to create Kubernetes client")
		os.Exit(1)
	}

	// Build gang discoverer chain based on configuration
	gangDiscoverer := buildGangDiscoverer(k8sClient, gangDiscoverers)
	log.Info("Initialized gang discoverers", "discoverers", gangDiscoverers)

	// Build injector configuration
	injectorConfig := InjectorConfig{
		PreflightImage:          preflightImage,
		DCGMHostengineAddr:      dcgmHostengineAddr,
		DCGMDiagLevel:           dcgmDiagLevel,
		PlatformConnectorSocket: platformConnectorSocket,
		GPUResourceNames:        gpuResourceNames,
		NCCLEnabled:             ncclEnabled,
		NCCLImage:               ncclImage,
		NCCLBWThreshold:         ncclBWThreshold,
		NCCLNProcsPerNode:       ncclNProcsPerNode,
		NCCLMasterPort:          ncclMasterPort,
	}

	log.Info("Injector configuration",
		"preflight_image", injectorConfig.PreflightImage,
		"dcgm_hostengine_addr", injectorConfig.DCGMHostengineAddr,
		"dcgm_diag_level", injectorConfig.DCGMDiagLevel,
		"platform_connector_socket", injectorConfig.PlatformConnectorSocket,
		"nccl_enabled", injectorConfig.NCCLEnabled,
		"nccl_image", injectorConfig.NCCLImage,
		"nccl_bw_threshold", injectorConfig.NCCLBWThreshold,
	)

	// Create webhook server
	srv := webhook.NewServer(webhook.Options{
		Port:     port,
		CertDir:  certDir,
		CertName: certName,
		KeyName:  keyName,
	})

	// Create admission handler
	decoder := admission.NewDecoder(scheme)
	handler := &InitContainerInjector{
		Decoder:        decoder,
		Client:         k8sClient,
		GangDiscoverer: gangDiscoverer,
		Log:            log.WithName("webhook"),
		Config:         injectorConfig,
	}

	// Register webhook endpoints
	srv.Register("/mutate", &admission.Webhook{Handler: handler})
	srv.Register("/mutate-pod", &admission.Webhook{Handler: handler})
	srv.Register("/healthz", http.HandlerFunc(healthzHandler))
	srv.Register("/readyz", http.HandlerFunc(readyzHandler))

	// Start server with graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Info("Starting preflight injector webhook server", "port", port)
	if err := srv.Start(ctx); err != nil {
		log.Error(err, "Failed to start webhook server")
		os.Exit(1)
	}
}

// buildGangDiscoverer creates the gang discoverer chain based on configuration.
func buildGangDiscoverer(k8sClient client.Client, config string) gang.GangDiscoverer {
	var discoverers []gang.GangDiscoverer

	// Default: Volcano first (since user is using Volcano), then workloadRef as fallback
	discoverers = append(discoverers,
		gang.NewVolcanoGangDiscoverer(k8sClient),
		gang.NewWorkloadRefGangDiscoverer(k8sClient),
	)

	return gang.NewCompositeGangDiscoverer(discoverers...)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func readyzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
