/*
 * Copyright 2025 The ChaosBlade Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pod

import (
	"regexp"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func boolPtr(b bool) *bool {
	return &b
}

func TestCollectConfigMapReferences(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		wantLen  int
		wantRefs []ConfigMapRef
	}{
		{
			name: "volume configmap only",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "config-vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "app-config"},
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "app-config", Optional: false, Source: "volume"},
			},
		},
		{
			name: "envFrom configmap optional",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "main",
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{Name: "env-config"},
										Optional:             boolPtr(true),
									},
								},
							},
						},
					},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "env-config", Optional: true, Source: "envFrom"},
			},
		},
		{
			name: "env valueFrom configmap",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "main",
							Env: []v1.EnvVar{
								{
									Name: "MY_VAR",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "key-config"},
										},
									},
								},
							},
						},
					},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "key-config", Optional: false, Source: "envValueFrom"},
			},
		},
		{
			name: "mixed sources with deduplication - required wins",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "shared-config"},
									Optional:             boolPtr(true),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name: "main",
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{Name: "shared-config"},
									},
								},
							},
						},
					},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "shared-config", Optional: false, Source: "volume"},
			},
		},
		{
			name: "no configmap references",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "main"},
					},
				},
			},
			wantLen:  0,
			wantRefs: nil,
		},
		{
			name: "init container configmap",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name: "init",
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{Name: "init-config"},
									},
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "init-config", Optional: false, Source: "envFrom"},
			},
		},
		{
			name: "multiple distinct configmaps ordered volume > envFrom > envValueFrom",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "vol-cm"},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name: "main",
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{Name: "env-cm"},
									},
								},
							},
							Env: []v1.EnvVar{
								{
									Name: "VAR",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "key-cm"},
										},
									},
								},
							},
						},
					},
				},
			},
			wantLen: 3,
			wantRefs: []ConfigMapRef{
				{Name: "vol-cm", Optional: false, Source: "volume"},
				{Name: "env-cm", Optional: false, Source: "envFrom"},
				{Name: "key-cm", Optional: false, Source: "envValueFrom"},
			},
		},
		{
			name: "volume with explicit optional false is required",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "explicit-required"},
									Optional:             boolPtr(false),
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			wantLen: 1,
			wantRefs: []ConfigMapRef{
				{Name: "explicit-required", Optional: false, Source: "volume"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectConfigMapReferences(tt.pod)
			if len(got) != tt.wantLen {
				t.Fatalf("collectConfigMapReferences() returned %d refs, want %d", len(got), tt.wantLen)
			}
			for i, want := range tt.wantRefs {
				if got[i].Name != want.Name {
					t.Errorf("ref[%d].Name = %q, want %q", i, got[i].Name, want.Name)
				}
				if got[i].Optional != want.Optional {
					t.Errorf("ref[%d].Optional = %v, want %v", i, got[i].Optional, want.Optional)
				}
				if got[i].Source != want.Source {
					t.Errorf("ref[%d].Source = %q, want %q", i, got[i].Source, want.Source)
				}
			}
		})
	}
}

func TestResolveTargetConfigMap(t *testing.T) {
	tests := []struct {
		name              string
		pod               *v1.Pod
		userSpecifiedName string
		wantCM            string
		wantErr           string
	}{
		{
			name: "user specifies valid required configmap",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "my-config"},
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "my-config",
			wantCM:            "my-config",
		},
		{
			name: "user specifies optional configmap - error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "opt-config"},
									Optional:             boolPtr(true),
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "opt-config",
			wantErr:           "optional",
		},
		{
			name: "user specifies non-existent configmap - error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "real-config"},
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "not-exist",
			wantErr:           "not referenced",
		},
		{
			name: "auto select first required configmap",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol1",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "first-config"},
								},
							},
						},
						{
							Name: "vol2",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "second-config"},
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "",
			wantCM:            "first-config",
		},
		{
			name: "auto select skips optional - picks required",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "optional-config"},
									Optional:             boolPtr(true),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name: "main",
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{Name: "required-config"},
									},
								},
							},
						},
					},
				},
			},
			userSpecifiedName: "",
			wantCM:            "required-config",
		},
		{
			name: "all optional - error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{Name: "opt-config"},
									Optional:             boolPtr(true),
								},
							},
						},
					},
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "",
			wantErr:           "no required",
		},
		{
			name: "no configmap at all - error",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{Name: "main"}},
				},
			},
			userSpecifiedName: "",
			wantErr:           "no ConfigMap dependency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTargetConfigMap(tt.pod, tt.userSpecifiedName)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("resolveTargetConfigMap() returned nil error, want error containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("resolveTargetConfigMap() error = %q, want to contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTargetConfigMap() returned unexpected error: %v", err)
			}
			if got != tt.wantCM {
				t.Errorf("resolveTargetConfigMap() = %q, want %q", got, tt.wantCM)
			}
		})
	}
}

func TestGetBackupConfigMapName(t *testing.T) {
	tests := []struct {
		name         string
		experimentId string
		namespace    string
		cmName       string
	}{
		{
			name:         "basic",
			experimentId: "exp-12345",
			namespace:    "default",
			cmName:       "my-config",
		},
		{
			name:         "with uuid",
			experimentId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			namespace:    "production",
			cmName:       "app-settings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result1 := getBackupConfigMapName(tt.experimentId, tt.namespace, tt.cmName)
			result2 := getBackupConfigMapName(tt.experimentId, tt.namespace, tt.cmName)

			// Deterministic
			if result1 != result2 {
				t.Errorf("getBackupConfigMapName() not deterministic: %q != %q", result1, result2)
			}

			// Valid DNS subdomain name (lowercase alphanumeric and hyphens)
			validDNS := regexp.MustCompile(`^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$`)
			if !validDNS.MatchString(result1) {
				t.Errorf("getBackupConfigMapName() result %q is not a valid DNS subdomain", result1)
			}

			// Within K8s name limit
			if len(result1) > 253 {
				t.Errorf("getBackupConfigMapName() result length %d exceeds 253", len(result1))
			}
		})
	}

	// Different inputs produce different outputs
	r1 := getBackupConfigMapName("exp1", "ns1", "cm1")
	r2 := getBackupConfigMapName("exp1", "ns1", "cm2")
	r3 := getBackupConfigMapName("exp1", "ns2", "cm1")
	r4 := getBackupConfigMapName("exp2", "ns1", "cm1")

	if r1 == r2 {
		t.Errorf("different cmName should produce different results: %q == %q", r1, r2)
	}
	if r1 == r3 {
		t.Errorf("different namespace should produce different results: %q == %q", r1, r3)
	}
	if r1 == r4 {
		t.Errorf("different experimentId should produce different results: %q == %q", r1, r4)
	}

	// ExperimentId truncation: short IDs should not be truncated
	shortResult := getBackupConfigMapName("abc", "ns", "cm")
	if !strings.Contains(shortResult, "abc") {
		t.Errorf("short experimentId should appear in name, got %q", shortResult)
	}

	// ExperimentId truncation: long IDs should be truncated to 8 chars
	longId := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	longResult := getBackupConfigMapName(longId, "ns", "cm")
	if strings.Contains(longResult, longId) {
		t.Errorf("long experimentId should be truncated, but full ID found in %q", longResult)
	}
	if !strings.Contains(longResult, longId[:8]) {
		t.Errorf("long experimentId should keep first 8 chars %q, got %q", longId[:8], longResult)
	}

	// Name length is bounded regardless of input lengths
	veryLongId := strings.Repeat("x", 200)
	boundedResult := getBackupConfigMapName(veryLongId, "ns", "cm")
	// "chaosblade-backup-" (18) + 8 (expId) + "-" (1) + 8 (hash) = 35
	if len(boundedResult) > 35 {
		t.Errorf("name should be bounded, got length %d: %q", len(boundedResult), boundedResult)
	}
}

func TestIsOptional(t *testing.T) {
	tests := []struct {
		name string
		opt  *bool
		want bool
	}{
		{"nil is required", nil, false},
		{"true is optional", boolPtr(true), true},
		{"false is required", boolPtr(false), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isOptional(tt.opt)
			if got != tt.want {
				t.Errorf("isOptional(%v) = %v, want %v", tt.opt, got, tt.want)
			}
		})
	}
}

func TestCopyStringMap(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		got := copyStringMap(nil)
		if got != nil {
			t.Errorf("copyStringMap(nil) = %v, want nil", got)
		}
	})

	t.Run("empty map returns empty map", func(t *testing.T) {
		got := copyStringMap(map[string]string{})
		if got == nil || len(got) != 0 {
			t.Errorf("copyStringMap(empty) = %v, want empty non-nil map", got)
		}
	})

	t.Run("copies all entries", func(t *testing.T) {
		src := map[string]string{"a": "1", "b": "2"}
		got := copyStringMap(src)
		if len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
			t.Errorf("copyStringMap() = %v, want %v", got, src)
		}
	})

	t.Run("modifying copy does not affect original", func(t *testing.T) {
		src := map[string]string{"key": "val"}
		dst := copyStringMap(src)
		dst["key"] = "changed"
		dst["new"] = "added"
		if src["key"] != "val" {
			t.Errorf("original was modified: src[key] = %q, want %q", src["key"], "val")
		}
		if _, exists := src["new"]; exists {
			t.Error("original gained new key from copy modification")
		}
	})
}

func TestCopyByteMap(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		got := copyByteMap(nil)
		if got != nil {
			t.Errorf("copyByteMap(nil) = %v, want nil", got)
		}
	})

	t.Run("empty map returns empty map", func(t *testing.T) {
		got := copyByteMap(map[string][]byte{})
		if got == nil || len(got) != 0 {
			t.Errorf("copyByteMap(empty) = %v, want empty non-nil map", got)
		}
	})

	t.Run("copies all entries", func(t *testing.T) {
		src := map[string][]byte{"a": {1, 2, 3}, "b": {4, 5}}
		got := copyByteMap(src)
		if len(got) != 2 {
			t.Fatalf("copyByteMap() returned %d entries, want 2", len(got))
		}
		if len(got["a"]) != 3 || got["a"][0] != 1 || got["a"][2] != 3 {
			t.Errorf("copyByteMap()[a] = %v, want [1 2 3]", got["a"])
		}
	})

	t.Run("deep copy - modifying byte slice does not affect original", func(t *testing.T) {
		original := []byte{10, 20, 30}
		src := map[string][]byte{"data": original}
		dst := copyByteMap(src)

		// Modify the copy
		dst["data"][0] = 99

		// Original should be unchanged
		if src["data"][0] != 10 {
			t.Errorf("original byte slice was modified: src[data][0] = %d, want 10", src["data"][0])
		}
	})
}
