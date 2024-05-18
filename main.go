package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/homedir"
)

// Define the taint key and value to apply
const (
	taintKey   = "node-tainter.atlas.io/unhealthy"
	taintValue = "true"
	leaseName  = "node-tainter-lease"
	lockName   = "node-tainter"
)

var (
	candidateNodes []corev1.Node
	candidateLock  sync.Mutex
)

func getenv(key string, fallback int) int {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	n, _ := strconv.Atoi(value)
	return n
}

func getKubeClient() *kubernetes.Clientset {
	var err error
	var config *rest.Config
	if home := homedir.HomeDir(); home != "" {
		config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(home, ".kube", "config"))
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	return clientset
}

func main() {
	leaseDuration := getenv("LEASE_DURATION", 30)
	renewalDeadline := getenv("RENEWAL_DEADLINE", 15)
	retryPeriod := getenv("RETRY_PERIOD", 10)
	leaseNamespace := os.Getenv("NAMESPACE")
	clientset := getKubeClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// process all nodes in the queue
	ticker := time.NewTicker(10 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				candidateLock.Lock()
				fmt.Printf("Processing [%d] nodes in the queue.\n", len(candidateNodes))
				if len(candidateNodes) > 0 {
					handleNode(ctx, &candidateNodes[0], clientset)
					candidateNodes = candidateNodes[1:]
				}
				candidateLock.Unlock()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		// Create a leaderElectionConfig for leader election
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      lockName,
				Namespace: leaseNamespace,
			},
			Client: clientset.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: os.Getenv("HOSTNAME"),
			},
		},
		LeaseDuration: time.Duration(leaseDuration) * time.Second,
		RenewDeadline: time.Duration(renewalDeadline) * time.Second,
		RetryPeriod:   time.Duration(retryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: onStartedLeading,
			OnStoppedLeading: func() { /* do nothing */ },
		},
		ReleaseOnCancel: true,
	}

	// block until the go routine ends
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Start the leader election
		leaderelection.RunOrDie(ctx, leaderElectionConfig)
	}()

	wg.Wait()
}

func onStartedLeading(ctx context.Context) {
	// Create a kubernetes client
	var config *rest.Config
	var err error
	if home := homedir.HomeDir(); home != "" {
		config, err = clientcmd.BuildConfigFromFlags("", filepath.Join(home, ".kube", "config"))
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// get all nodes , and add it to the watch list
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		candidateNodes = append(candidateNodes, nodes.Items...)
	}

	// Watch for node updates
	watch, err := clientset.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Println("Error creating node watcher:", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		node, ok := event.Object.(*corev1.Node)
		if !ok {
			fmt.Println("Unexpected object type:", event.Object.GetObjectKind().GroupVersionKind().String())
			continue
		}
		if hasUnhealthyCondition(node) && !isTaintedByNodeTainter(node) {
			candidateLock.Lock()
			fmt.Printf("Adding %s[version:%s] to the list of candidate nodes that must be tainted\n", node.Name, node.ResourceVersion)
			candidateNodes = append(candidateNodes, *node)
			candidateLock.Unlock()
		}
	}
}

func hasUnhealthyCondition(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {

		// Don't track the ready condition. It is supposed to be 'True' unlke
		// the other conditions which usually translate to a bad state
		if condition.Type == "Ready" {
			continue
		}

		if condition.Status == "True" {
			return true
		}
	}
	return false
}

func handleNode(ctx context.Context, node *corev1.Node, clientset *kubernetes.Clientset) {
	// Check if any of the node conditions is true
	for _, condition := range node.Status.Conditions {

		// Don't track the ready condition. It is supposed to be 'True' unlke
		// the other conditions which usually translate to a bad state
		if condition.Type == "Ready" {
			continue
		}

		if condition.Status == "True" {
			// fetch the node again so we have the latest version
			clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			processNode(node, clientset)
		}
	}
}

func isTaintedByNodeTainter(node *corev1.Node) bool {
	// If the node already has our taint, do nothing
	if slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.Key == taintKey
	}) {
		return true
	}

	return false
}

func processNode(node *corev1.Node, clientset *kubernetes.Clientset) {

	// If the node already has our taint, do nothing
	if isTaintedByNodeTainter(node) {
		return
	}

	for _, condition := range node.Status.Conditions {

		// Don't track the ready condition. It is supposed to be 'True' unlke
		// the other conditions which usually translate to a bad state
		if condition.Type == "Ready" {
			continue
		}

		if condition.Status == "True" {

			fmt.Printf("Node %q has the condition %q enabled on it\n", node.Name, condition.Type)

			// Taint the node with the specified key and value
			// Create a new taint object
			taint := &corev1.Taint{
				Key:    taintKey,
				Value:  taintValue,
				Effect: corev1.TaintEffectNoSchedule,
			}

			// Add the taint to the node spec
			node.Spec.Taints = append(node.Spec.Taints, *taint)
			// Update the node with the new spec
			_, err := clientset.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
			if err != nil {
				fmt.Println("Error tainting node:", err)
			} else {
				fmt.Println("Tainted node:", node.Name)
			}
		}
	}
}
