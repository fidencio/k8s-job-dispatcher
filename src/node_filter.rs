// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! DaemonSet-equivalent node admission.
//!
//! Pinning a pod with `spec.nodeName` bypasses the scheduler, and `NoSchedule` is
//! a scheduler-side check the kubelet never repeats, so taint admission has to be
//! re-implemented here or a run would reach nodes an equivalent DaemonSet would
//! have skipped.

use k8s_openapi::api::core::v1::{Node, Taint, Toleration};

/// `PreferNoSchedule` expresses a preference and never blocks admission.
const BLOCKING_EFFECTS: [&str; 2] = ["NoSchedule", "NoExecute"];

/// What the Job template's pod spec says about where its pods may run.
#[derive(Debug, Default, Clone)]
pub struct PodAdmission {
    pub tolerations: Vec<Toleration>,
    pub host_network: bool,
}

pub struct SkippedNode {
    pub name: String,
    pub taint: Taint,
}

/// Spelled the way `kubectl taint` does, for logs and errors.
pub fn describe_taint(taint: &Taint) -> String {
    match taint.value.as_deref() {
        Some(value) if !value.is_empty() => {
            format!("{}={}:{}", taint.key, value, taint.effect)
        }
        _ => format!("{}:{}", taint.key, taint.effect),
    }
}

pub fn suggested_toleration(taint: &Taint) -> String {
    format!(
        "tolerations:\n  - key: {}\n    operator: Exists\n    effect: {}",
        taint.key, taint.effect
    )
}

/// Tolerations the DaemonSet controller adds silently, without which a cordoned
/// or resource-pressured node would miss a rollout a DaemonSet would have reached.
/// `network-unavailable` is in that set only for host-network pods.
fn daemonset_controller_tolerations(host_network: bool) -> Vec<Toleration> {
    let mut implicit = vec![
        ("node.kubernetes.io/not-ready", "NoExecute"),
        ("node.kubernetes.io/unreachable", "NoExecute"),
        ("node.kubernetes.io/disk-pressure", "NoSchedule"),
        ("node.kubernetes.io/memory-pressure", "NoSchedule"),
        ("node.kubernetes.io/pid-pressure", "NoSchedule"),
        ("node.kubernetes.io/unschedulable", "NoSchedule"),
    ];
    if host_network {
        implicit.push(("node.kubernetes.io/network-unavailable", "NoSchedule"));
    }

    implicit
        .into_iter()
        .map(|(key, effect)| Toleration {
            key: Some(key.to_string()),
            operator: Some("Exists".to_string()),
            effect: Some(effect.to_string()),
            ..Default::default()
        })
        .collect()
}

/// Follows Kubernetes' own matching rules: an empty effect matches any effect, an
/// empty key any key, and the operator defaults to `Equal`.
pub fn tolerates(toleration: &Toleration, taint: &Taint) -> bool {
    if let Some(effect) = toleration.effect.as_deref() {
        if !effect.is_empty() && effect != taint.effect {
            return false;
        }
    }
    if let Some(key) = toleration.key.as_deref() {
        if !key.is_empty() && key != taint.key {
            return false;
        }
    }

    match toleration.operator.as_deref().unwrap_or("Equal") {
        "Exists" => true,
        _ => toleration.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or(""),
    }
}

pub fn untolerated_taint<'a>(taints: &'a [Taint], tolerations: &[Toleration]) -> Option<&'a Taint> {
    taints.iter().find(|taint| {
        BLOCKING_EFFECTS.contains(&taint.effect.as_str())
            && !tolerations.iter().any(|tol| tolerates(tol, taint))
    })
}

pub fn partition_by_tolerations(
    nodes: &[Node],
    admission: &PodAdmission,
) -> (Vec<String>, Vec<SkippedNode>) {
    let mut effective = admission.tolerations.clone();
    effective.extend(daemonset_controller_tolerations(admission.host_network));

    let mut admitted = Vec::new();
    let mut skipped = Vec::new();

    for node in nodes {
        let Some(name) = node.metadata.name.clone() else {
            continue;
        };
        let taints = node
            .spec
            .as_ref()
            .and_then(|spec| spec.taints.as_deref())
            .unwrap_or(&[]);

        match untolerated_taint(taints, &effective) {
            Some(taint) => skipped.push(SkippedNode {
                name,
                taint: taint.clone(),
            }),
            None => admitted.push(name),
        }
    }

    // A node matching several selectors arrives once per match, and both counts
    // are reported to the user.
    admitted.sort();
    admitted.dedup();
    skipped.sort_by(|a, b| a.name.cmp(&b.name));
    skipped.dedup_by(|a, b| a.name == b.name);
    (admitted, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::NodeSpec;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn taint(key: &str, value: Option<&str>, effect: &str) -> Taint {
        Taint {
            key: key.to_string(),
            value: value.map(str::to_string),
            effect: effect.to_string(),
            ..Default::default()
        }
    }

    fn toleration(key: Option<&str>, operator: &str, effect: Option<&str>) -> Toleration {
        Toleration {
            key: key.map(str::to_string),
            operator: Some(operator.to_string()),
            effect: effect.map(str::to_string),
            ..Default::default()
        }
    }

    fn node(name: &str, taints: Vec<Taint>) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: Some(NodeSpec {
                taints: Some(taints),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn admission(tolerations: &[Toleration]) -> PodAdmission {
        PodAdmission {
            tolerations: tolerations.to_vec(),
            host_network: false,
        }
    }

    fn control_plane_taint() -> Taint {
        taint("node-role.kubernetes.io/control-plane", None, "NoSchedule")
    }

    #[test]
    fn a_node_matched_by_several_selectors_is_reported_once() {
        let admitted_twice = node("worker-1", vec![]);
        let skipped_twice = node("cp-1", vec![control_plane_taint()]);
        let nodes = vec![
            admitted_twice.clone(),
            skipped_twice.clone(),
            admitted_twice,
            skipped_twice,
        ];

        let (admitted, skipped) = partition_by_tolerations(&nodes, &admission(&[]));

        assert_eq!(admitted, vec!["worker-1".to_string()]);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "cp-1");
    }

    #[test]
    fn exists_toleration_matches_regardless_of_value() {
        let tol = toleration(
            Some("node-role.kubernetes.io/control-plane"),
            "Exists",
            Some("NoSchedule"),
        );
        assert!(tolerates(&tol, &control_plane_taint()));
    }

    #[test]
    fn empty_key_with_exists_tolerates_everything() {
        let tol = toleration(None, "Exists", None);
        assert!(tolerates(&tol, &control_plane_taint()));
        assert!(tolerates(&tol, &taint("custom", Some("v"), "NoExecute")));
    }

    #[test]
    fn effect_must_match_when_specified() {
        let tol = toleration(
            Some("node-role.kubernetes.io/control-plane"),
            "Exists",
            Some("NoExecute"),
        );
        assert!(!tolerates(&tol, &control_plane_taint()));
    }

    #[test]
    fn equal_operator_compares_values() {
        let mut tol = toleration(Some("dedicated"), "Equal", None);
        tol.value = Some("batch".to_string());

        assert!(tolerates(
            &tol,
            &taint("dedicated", Some("batch"), "NoSchedule")
        ));
        assert!(!tolerates(
            &tol,
            &taint("dedicated", Some("other"), "NoSchedule")
        ));
    }

    #[test]
    fn prefer_no_schedule_never_blocks() {
        let taints = vec![taint("spot", None, "PreferNoSchedule")];
        assert!(untolerated_taint(&taints, &[]).is_none());
    }

    #[test]
    fn control_plane_node_is_skipped_without_a_toleration() {
        let nodes = vec![
            node("worker-1", vec![]),
            node("cp-1", vec![control_plane_taint()]),
        ];

        let (admitted, skipped) = partition_by_tolerations(&nodes, &admission(&[]));

        assert_eq!(admitted, vec!["worker-1"]);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "cp-1");
        assert_eq!(
            describe_taint(&skipped[0].taint),
            "node-role.kubernetes.io/control-plane:NoSchedule"
        );
    }

    #[test]
    fn control_plane_node_is_admitted_once_tolerated() {
        let nodes = vec![node("cp-1", vec![control_plane_taint()])];
        let tolerations = vec![toleration(
            Some("node-role.kubernetes.io/control-plane"),
            "Exists",
            Some("NoSchedule"),
        )];

        let (admitted, skipped) = partition_by_tolerations(&nodes, &admission(&tolerations));

        assert_eq!(admitted, vec!["cp-1"]);
        assert!(skipped.is_empty());
    }

    #[test]
    fn cordoned_and_pressured_nodes_stay_admitted_like_a_daemonset() {
        let nodes = vec![
            node(
                "cordoned",
                vec![taint(
                    "node.kubernetes.io/unschedulable",
                    None,
                    "NoSchedule",
                )],
            ),
            node(
                "pressured",
                vec![taint(
                    "node.kubernetes.io/memory-pressure",
                    None,
                    "NoSchedule",
                )],
            ),
        ];

        let (admitted, skipped) = partition_by_tolerations(&nodes, &admission(&[]));

        assert_eq!(admitted, vec!["cordoned", "pressured"]);
        assert!(skipped.is_empty());
    }

    #[test]
    fn network_unavailable_is_tolerated_only_for_host_network_pods() {
        let nodes = vec![node(
            "no-network",
            vec![taint(
                "node.kubernetes.io/network-unavailable",
                None,
                "NoSchedule",
            )],
        )];

        let (admitted, skipped) = partition_by_tolerations(&nodes, &admission(&[]));
        assert!(admitted.is_empty());
        assert_eq!(skipped.len(), 1);

        let host_networked = PodAdmission {
            tolerations: Vec::new(),
            host_network: true,
        };
        let (admitted, skipped) = partition_by_tolerations(&nodes, &host_networked);
        assert_eq!(admitted, vec!["no-network"]);
        assert!(skipped.is_empty());
    }

    #[test]
    fn nodes_without_a_spec_or_taints_are_admitted() {
        let bare = Node {
            metadata: ObjectMeta {
                name: Some("bare".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let (admitted, skipped) = partition_by_tolerations(&[bare], &admission(&[]));

        assert_eq!(admitted, vec!["bare"]);
        assert!(skipped.is_empty());
    }

    #[test]
    fn suggested_toleration_names_the_blocking_taint() {
        let suggestion = suggested_toleration(&control_plane_taint());

        assert!(suggestion.contains("key: node-role.kubernetes.io/control-plane"));
        assert!(suggestion.contains("effect: NoSchedule"));
    }

    #[test]
    fn describe_taint_includes_the_value_when_present() {
        assert_eq!(
            describe_taint(&taint("dedicated", Some("batch"), "NoSchedule")),
            "dedicated=batch:NoSchedule"
        );
    }
}
