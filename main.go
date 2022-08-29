package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	clientcmd "k8s.io/client-go/tools/clientcmd"
)

func connectToK8s() *kubernetes.Clientset {
	home, exists := os.LookupEnv("HOME")
	if !exists {
		home = "/root"
	}

	configPath := filepath.Join(home, ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		log.Fatalln("failed to create K8s config")
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln("Failed to create K8s clientset")
	}

	return clientset
}

type JobDetail struct {
	JobName    string
	Image      string
	RequestMem string
	RequestCpu string
}

type BatchJob struct {
	BatchJob1 []JobDetail
}

func main() {

	var batchJob BatchJob
	jobString := []byte(`
	{
		"batchJob1": [
			{
				"jobName": "job1",
				"image": "docker_img_1",
				"requestMem": "500Mi",
				"requestCpu": "200m"
			},
			{
				"jobName": "job2",
				"image": "docker_img_2",
				"requestMem": "1Gi",
				"requestCpu": "100m"
			},
			{
				"jobName": "job3",
				"image": "docker_img_3",
				"requestMem": "2Gi",
				"requestCpu": "200m"
			}
		]
	}	
`)

	err := json.Unmarshal(jobString, &batchJob)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(batchJob)

	clientset := connectToK8s()

	for _, element := range batchJob.BatchJob1 {
		// For Sceneario 1 & 2 run functionn `k8sJobwithOutAnitiAffinity` soft place into same node
		// For Sceneario 3 run functionn `k8sJobwithAnitiAffinity` soft only job1 run on different node

		k8sJobwithOutAnitiAffinity(clientset, &element.JobName, &element.Image, &element.RequestMem, &element.RequestCpu)
		// k8sJobwithAnitiAffinity(clientset, &element.JobName, &element.Image, &element.RequestMem, &element.RequestCpu)
	}
}

func k8sJobwithOutAnitiAffinity(clientset *kubernetes.Clientset, jobName *string, image *string, requestMem *string, requestCpu *string) {
	jobs := clientset.BatchV1().Jobs("default")
	var backOffLimit int32 = 0

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *jobName,
			Namespace: "default",
		},

		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: v1.PodAffinityTerm{
										TopologyKey: "kubernetes.io/hostname",
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "job-name",
													Operator: "In",
													Values:   []string{"job1", "job2", "job3"},
												},
											},
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:            *jobName,
							Image:           *image,
							ImagePullPolicy: "IfNotPresent",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse(*requestCpu),
									"memory": resource.MustParse(*requestMem),
								},
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse(*requestCpu),
									"memory": resource.MustParse(*requestMem),
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
			BackoffLimit: &backOffLimit,
		},
	}

	_, err := jobs.Create(context.TODO(), jobSpec, metav1.CreateOptions{})
	if err != nil {
		log.Fatalln("Scenario 3: Failed to create k8s job")
	}
	log.Println("Scenario 3: Created k8s job successfully")
}

func k8sJobwithAnitiAffinity(clientset *kubernetes.Clientset, jobName *string, image *string, requestMem *string, requestCpu *string) {
	jobs := clientset.BatchV1().Jobs("default")
	var backOffLimit int32 = 0

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *jobName,
			Namespace: "default",
		},

		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: v1.PodAffinityTerm{
										TopologyKey: "kubernetes.io/hostname",
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "job-name",
													Operator: "In",
													Values:   []string{"job1"},
												},
											},
										},
									},
								},
							},
						},
						PodAffinity: &v1.PodAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: v1.PodAffinityTerm{
										TopologyKey: "kubernetes.io/hostname",
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "job-name",
													Operator: "In",
													Values:   []string{"job2", "job3"},
												},
											},
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:            *jobName,
							Image:           *image,
							ImagePullPolicy: "IfNotPresent",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse(*requestCpu),
									"memory": resource.MustParse(*requestMem),
								},
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse(*requestCpu),
									"memory": resource.MustParse(*requestMem),
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
			BackoffLimit: &backOffLimit,
		},
	}

	_, err := jobs.Create(context.TODO(), jobSpec, metav1.CreateOptions{})
	if err != nil {
		log.Fatalln("Scenario 3: Failed to create k8s job")
	}
	log.Println("Scenario 3: Created k8s job successfully")
}
