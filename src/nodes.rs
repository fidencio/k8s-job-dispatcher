// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! The node-scoped API work, done here rather than inside the per-node Jobs so
//! that those Jobs need no ServiceAccount token: they are the privileged half of
//! a rollout and they run on every targeted node, where root can read any token
//! mounted into a pod. The dispatcher is one unprivileged pod instead.

use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::{Node, Taint};
use kube::api::{Api, GetParams, Patch, PatchParams, Request};
use kube::Client;
use log::{info, warn};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub const DEFAULT_PENDING_LABEL_VALUE: &str = "false";

const DEFAULT_INSTANCE: &str = "default";

/// The kubelet republishes node status every ~10s, and `runtimeHandlers` trails a
/// runtime restart by a sync or two.
const HANDLER_WAIT: Duration = Duration::from_secs(120);

/// A kubelet re-registering after a CRI restart republishes its cached labels over
/// ours, so one confirmation proves nothing. Six spaced ones outlive a
/// status-update period.
const LABEL_STABILITY_CHECKS: u32 = 6;
const LABEL_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const LABEL_APPLY_ATTEMPTS: u32 = 12;

// Concurrent updates are lost races, not broken nodes.
const TAINT_PATCH_ATTEMPTS: u32 = 3;
const CLAIM_PATCH_ATTEMPTS: u32 = 3;
const LABEL_PATCH_ATTEMPTS: u32 = 5;

/// The kubelet may be unreachable through the apiserver proxy, and the check using
/// this is advisory, so it must not hold up the queue behind it.
const KUBELET_PROBE_TIMEOUT: Duration = Duration::from_secs(15);

/// One instance's marker label. Without it, an instance removing the shared label
/// could not tell whether that leaves another instance's workloads with nowhere to
/// run.
#[derive(Debug, Clone)]
pub struct InstanceMarker {
    prefix: String,
    key: String,
}

impl InstanceMarker {
    pub fn new(prefix: &str, instance: Option<&str>) -> Self {
        let prefix = prefix.trim().trim_end_matches('/').to_string();
        let instance = instance
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or(DEFAULT_INSTANCE);

        Self {
            key: format!("{prefix}/{instance}"),
            prefix,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Matched on the `/` boundary, so an unrelated key that merely starts with
    /// the same characters is not read as another instance's claim.
    fn is_marker(&self, key: &str) -> bool {
        key.len() > self.prefix.len()
            && key.starts_with(&self.prefix)
            && key.as_bytes()[self.prefix.len()] == b'/'
    }
}

/// The key is the caller's, because whatever selects on it is theirs.
#[derive(Debug, Clone)]
pub struct NodeLabelling {
    pub key: String,
    pub pending_value: String,
    pub instance: Option<InstanceMarker>,
}

impl NodeLabelling {
    /// The key recording that this instance holds a node: its marker when there
    /// is one, otherwise the shared key, which a lone instance owns outright.
    pub fn ownership_key(&self) -> &str {
        self.instance
            .as_ref()
            .map_or(self.key.as_str(), InstanceMarker::key)
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = vec![self.key.as_str()];
        if let Some(instance) = self.instance.as_ref() {
            keys.push(instance.key());
        }
        keys
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SharedLabel {
    /// Another instance is still serving here.
    Keep,
    /// The others have only claimed the node: nothing may be selected on the
    /// shared key, but it has to stay so their cleanups can still find the node.
    Demote,
    /// Ours was the last mark.
    Remove,
}

/// The value matters, not just the key: reading a pending mark as "ready here"
/// would leave the node advertised with nothing behind it.
fn shared_label_after(labels: &BTreeMap<String, String>, labelling: &NodeLabelling) -> SharedLabel {
    let Some(instance) = labelling.instance.as_ref() else {
        return SharedLabel::Remove;
    };

    let mut any = false;
    for (key, value) in labels {
        if key == instance.key() || !instance.is_marker(key) {
            continue;
        }
        any = true;
        if value != &labelling.pending_value {
            return SharedLabel::Keep;
        }
    }

    if any {
        SharedLabel::Demote
    } else {
        SharedLabel::Remove
    }
}

/// What a per-node Job would otherwise have to read from the apiserver itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeFacts {
    pub name: String,
    pub container_runtime_version: Option<String>,
    pub machine_id: Option<String>,
}

impl NodeFacts {
    pub fn from_node(node: &Node) -> Self {
        let name = node.metadata.name.clone().unwrap_or_default();
        let info = node
            .status
            .as_ref()
            .and_then(|status| status.node_info.as_ref());
        let non_empty = |value: String| Some(value).filter(|value| !value.is_empty());

        Self {
            name,
            container_runtime_version: info
                .and_then(|info| non_empty(info.container_runtime_version.clone())),
            machine_id: info.and_then(|info| non_empty(info.machine_id.clone())),
        }
    }
}

#[derive(Clone)]
pub struct NodeOps {
    api: Api<Node>,
    client: Client,
    /// `None` leaves every node label alone.
    pub labelling: Option<NodeLabelling>,
    pub label_value: Option<String>,
    pub remove_label: bool,
    pub claim_pending: bool,
    /// Matchers, each `key` (any effect) or `key:effect`.
    pub remove_taints: Vec<String>,
    pub wait_ready: Option<Duration>,
    pub require_handlers: Vec<String>,
    pub kubelet_timeout_warn: Option<Duration>,
}

impl NodeOps {
    pub fn new(client: &Client) -> Self {
        Self {
            api: Api::all(client.clone()),
            client: client.clone(),
            labelling: None,
            label_value: None,
            remove_label: false,
            claim_pending: false,
            remove_taints: Vec::new(),
            wait_ready: None,
            require_handlers: Vec::new(),
            kubelet_timeout_warn: None,
        }
    }

    pub async fn get(&self, node: &str) -> Result<Node> {
        self.api
            .get(node)
            .await
            .with_context(|| format!("failed to get node {node}"))
    }

    /// A node name can be reused by a replacement machine, so every step that
    /// writes to a node is bracketed by the UID the dispatcher selected.
    async fn ensure_uid(&self, node: &str, expected_uid: &str) -> Result<()> {
        let current = self.get(node).await?;
        anyhow::ensure!(
            current.metadata.uid.as_deref() == Some(expected_uid),
            "node {node} changed identity: expected UID {expected_uid}, found {:?}",
            current.metadata.uid
        );
        Ok(())
    }

    pub async fn before_dispatch(&self, node: &str, expected_uid: &str) -> Result<()> {
        self.ensure_uid(node, expected_uid).await?;
        if self.remove_label {
            self.demote(node, expected_uid).await?;
        }
        if self.claim_pending {
            self.claim(node, expected_uid).await?;
        }
        if let Some(threshold) = self.kubelet_timeout_warn {
            self.warn_on_low_kubelet_timeout(node, threshold).await;
        }
        self.ensure_uid(node, expected_uid).await?;
        Ok(())
    }

    /// The order is the point: a node has to be Ready before it is advertised as
    /// ready, and its start-up taints may only be lifted once that advertisement
    /// is in place, since they are what keeps workloads off it until then.
    pub async fn after_success(&self, node: &str, expected_uid: &str) -> Result<()> {
        self.ensure_uid(node, expected_uid).await?;

        if self.remove_label {
            self.release(node, expected_uid).await?;
            return self.ensure_uid(node, expected_uid).await;
        }

        let Some(value) = self.label_value.clone() else {
            return Ok(());
        };

        if let Some(timeout) = self.wait_ready {
            self.wait_till_ready(node, timeout).await?;
        }

        self.ensure_uid(node, expected_uid).await?;
        self.verify_handlers(node).await?;
        self.ensure_uid(node, expected_uid).await?;
        self.label_until_stable(node, &value, expected_uid).await?;
        self.ensure_uid(node, expected_uid).await?;
        self.lift_taints(node, expected_uid).await;

        Ok(())
    }

    /// Demoted rather than removed, so that nothing new is selected onto the node
    /// while it is taken apart but a cleanup Job that fails can still be found by
    /// the next one. A node that never had the label is left alone.
    async fn demote(&self, node: &str, expected_uid: &str) -> Result<()> {
        self.rewrite_labels(node, expected_uid, |labels, labelling| {
            let mut updates: Vec<(String, Option<String>)> = Vec::new();
            let shared_key = labelling.key.as_str();
            let pending = labelling.pending_value.as_str();

            if let Some(instance) = labelling.instance.as_ref() {
                if labels.contains_key(instance.key()) {
                    updates.push((instance.key().to_string(), Some(pending.to_string())));
                }
            }

            match (
                labels.get(shared_key).map(String::as_str),
                shared_label_after(labels, labelling),
            ) {
                (None, _) => (),
                (Some(current), _) if current == pending => (),
                (Some(_), SharedLabel::Keep) => info!(
                    "node {node}: leaving {shared_key} in place, another instance is still \
                     serving from this node"
                ),
                (Some(_), _) => updates.push((shared_key.to_string(), Some(pending.to_string()))),
            }

            updates
        })
        .await
    }

    async fn release(&self, node: &str, expected_uid: &str) -> Result<()> {
        self.rewrite_labels(node, expected_uid, |labels, labelling| {
            let mut updates: Vec<(String, Option<String>)> = Vec::new();
            let shared_key = labelling.key.as_str();
            let pending = labelling.pending_value.as_str();

            if let Some(instance) = labelling.instance.as_ref() {
                if labels.contains_key(instance.key()) {
                    updates.push((instance.key().to_string(), None));
                }
            }

            match shared_label_after(labels, labelling) {
                SharedLabel::Keep => info!(
                    "node {node}: keeping {shared_key}, another instance is still serving from \
                     this node"
                ),
                SharedLabel::Demote => {
                    if labels.get(shared_key).map(String::as_str) != Some(pending) {
                        info!(
                            "node {node}: leaving {shared_key}={pending}, one or more instances \
                             have claimed this node but none has finished on it"
                        );
                        updates.push((shared_key.to_string(), Some(pending.to_string())));
                    }
                }
                SharedLabel::Remove => {
                    if labels.contains_key(shared_key) {
                        updates.push((shared_key.to_string(), None));
                    }
                }
            }

            updates
        })
        .await
    }

    /// Guarded read-modify-write. The decision is read from labels other instances
    /// write concurrently, so an unconditional write could act on a node that has
    /// moved on: two cleanups each seeing the other's mark, each removing their
    /// own, leaving a node advertising readiness with nothing behind it.
    async fn rewrite_labels<F>(&self, node: &str, expected_uid: &str, decide: F) -> Result<()>
    where
        F: Fn(&BTreeMap<String, String>, &NodeLabelling) -> Vec<(String, Option<String>)>,
    {
        let Some(labelling) = self.labelling.as_ref() else {
            return Ok(());
        };

        for attempt in 1..=LABEL_PATCH_ATTEMPTS {
            let fetched = self.get(node).await?;
            anyhow::ensure!(
                fetched.metadata.uid.as_deref() == Some(expected_uid),
                "node {node} changed identity before its labels could be rewritten"
            );
            let version = fetched
                .metadata
                .resource_version
                .clone()
                .unwrap_or_default();
            let labels = fetched.metadata.labels.unwrap_or_default();

            let updates = decide(&labels, labelling);
            if updates.is_empty() {
                return Ok(());
            }

            match self
                .patch_labels_guarded(node, expected_uid, &version, &updates)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) if err.is_conflict => {
                    info!(
                        "node {node}: its labels changed while they were being rewritten \
                         (attempt {attempt}/{LABEL_PATCH_ATTEMPTS}); reading them again"
                    );
                }
                Err(err) => return Err(err.error),
            }
        }

        anyhow::bail!(
            "gave up rewriting the labels on node {node} after {LABEL_PATCH_ATTEMPTS} attempts: \
             something keeps changing them concurrently"
        )
    }

    async fn patch_labels_guarded(
        &self,
        node: &str,
        expected_uid: &str,
        version: &str,
        updates: &[(String, Option<String>)],
    ) -> std::result::Result<(), GuardedPatchError> {
        let mut ops = vec![
            json!({"op": "test", "path": "/metadata/uid", "value": expected_uid}),
            json!({"op": "test", "path": "/metadata/resourceVersion", "value": version}),
        ];
        for (key, value) in updates {
            let path = format!("/metadata/labels/{}", escape_pointer(key));
            match value {
                Some(value) => ops.push(json!({"op": "add", "path": path, "value": value})),
                // `remove` fails when the key is already gone, which is another
                // way of saying we read a stale node.
                None => ops.push(json!({"op": "remove", "path": path})),
            }
        }

        let patch: json_patch::Patch =
            serde_json::from_value(json!(ops)).map_err(|err| GuardedPatchError {
                is_conflict: false,
                error: anyhow::Error::new(err).context("failed to build the label patch"),
            })?;

        match self
            .api
            .patch(node, &PatchParams::default(), &Patch::Json::<Node>(patch))
            .await
        {
            Ok(_) => {
                info!("node {node}: labels {}", describe_updates(updates));
                Ok(())
            }
            Err(err) => Err(GuardedPatchError {
                is_conflict: is_precondition_failure(&err),
                error: anyhow::Error::new(err).context(format!(
                    "failed to write labels {} on node {node}",
                    describe_updates(updates)
                )),
            }),
        }
    }

    /// Fatal when it cannot be done: the claim is what makes this node findable by
    /// a later cleanup, so mutating a host without it would leave one nothing can
    /// discover. Conditional on the label still being absent, because claiming a
    /// node another dispatcher has just finished labelling would de-advertise a
    /// node that is serving.
    async fn claim(&self, node: &str, expected_uid: &str) -> Result<()> {
        let Some(labelling) = self.labelling.as_ref() else {
            return Ok(());
        };
        let pending = labelling.pending_value.as_str();

        for attempt in 1..=CLAIM_PATCH_ATTEMPTS {
            let fetched = self
                .get(node)
                .await
                .with_context(|| format!("could not read node {node} to claim it"))?;
            anyhow::ensure!(
                fetched.metadata.uid.as_deref() == Some(expected_uid),
                "node {node} changed identity before it could be claimed"
            );

            let labels = fetched.metadata.labels.unwrap_or_default();
            // Any value means someone has been here: a finished value must not be
            // downgraded mid-upgrade, and a pending one is already the claim.
            let missing: Vec<&str> = labelling
                .keys()
                .into_iter()
                .filter(|key| !labels.contains_key(*key))
                .collect();
            if missing.is_empty() {
                return Ok(());
            }

            let version = fetched
                .metadata
                .resource_version
                .clone()
                .unwrap_or_default();
            let mut ops = vec![
                json!({"op": "test", "path": "/metadata/uid", "value": expected_uid}),
                json!({"op": "test", "path": "/metadata/resourceVersion", "value": version}),
            ];
            if labels.is_empty() {
                // `add` needs its parent to exist.
                let claimed: BTreeMap<&str, &str> =
                    missing.iter().map(|key| (*key, pending)).collect();
                ops.push(json!({"op": "add", "path": "/metadata/labels", "value": claimed}));
            } else {
                for key in &missing {
                    ops.push(json!({"op": "add",
                                    "path": format!("/metadata/labels/{}", escape_pointer(key)),
                                    "value": pending}));
                }
            }

            let patch: json_patch::Patch = serde_json::from_value(json!(ops))
                .context("could not build the node claim patch")?;

            match self
                .api
                .patch(node, &PatchParams::default(), &Patch::Json::<Node>(patch))
                .await
            {
                Ok(_) => {
                    info!("node {node}: marked as being worked on");
                    return Ok(());
                }
                Err(err) if is_precondition_failure(&err) => {
                    info!(
                        "node {node}: its labels changed while it was being claimed \
                         (attempt {attempt}/{CLAIM_PATCH_ATTEMPTS}); reading them again"
                    );
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "could not claim node {node}; refusing to mutate a host that a later \
                             cleanup could not discover"
                        )
                    })
                }
            }
        }

        anyhow::bail!(
            "gave up claiming node {node} after {CLAIM_PATCH_ATTEMPTS} attempts: something keeps \
             changing its labels; refusing to mutate a host that a later cleanup could not \
             discover"
        )
    }

    /// `.status.runtimeHandlers` is the node's own answer about what its runtime
    /// loaded, as opposed to what was written and hoped it would read. Asking needs
    /// the apiserver, which is why it cannot live in the per-node Jobs.
    async fn verify_handlers(&self, node: &str) -> Result<()> {
        if self.require_handlers.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        loop {
            let served = match self.get(node).await {
                Ok(fetched) => served_handlers(&fetched),
                // A failed request is not an answer, and giving up here would
                // label the node on the strength of one.
                Err(err) => {
                    if start.elapsed() >= HANDLER_WAIT {
                        return Err(err).with_context(|| {
                            format!(
                                "could not check whether node {node} is serving {:?} within {}s \
                                 of its Job finishing; not labelling it, since nothing has \
                                 confirmed its CRI runtime read what was installed",
                                self.require_handlers,
                                HANDLER_WAIT.as_secs()
                            )
                        });
                    }
                    warn!(
                        "node {node}: could not read its runtime handlers ({err:#}); trying again"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            match handler_verdict(&self.require_handlers, served.as_deref()) {
                HandlerVerdict::Serving { serving, missing } => {
                    info!("node {node}: CRI runtime is serving {serving:?}");
                    if !missing.is_empty() {
                        warn!(
                            "node {node}: CRI runtime is not serving {missing:?}. Expected for \
                             handlers built for another architecture; otherwise pods requesting \
                             them will not start on this node"
                        );
                    }
                    return Ok(());
                }
                // Never a reason to fail a run that otherwise worked.
                HandlerVerdict::Unanswerable => {
                    info!(
                        "node {node}: does not report runtime handlers at all (Kubernetes below \
                         1.30, or a kubelet that does not publish them), so what its runtime \
                         loaded cannot be checked from here"
                    );
                    return Ok(());
                }
                HandlerVerdict::NotServing if start.elapsed() >= HANDLER_WAIT => {
                    anyhow::bail!(
                        "node {node} reports none of {:?} among its runtime handlers {}s after its \
                         Job finished, so its CRI runtime is not serving the configuration that \
                         Job wrote. Check the runtime's logs for a rejected or unread \
                         configuration file; not labelling the node, since no pod asking for \
                         those handlers could run there",
                        self.require_handlers,
                        HANDLER_WAIT.as_secs()
                    )
                }
                HandlerVerdict::NotServing => (),
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// On k3s and RKE2 a CRI restart takes the kubelet with it, and a kubelet
    /// coming back re-registers its node with cached labels, silently undoing
    /// ours. `Ready` does not rule that out, since the observation can predate the
    /// kubelet's own restart, so the label has to be seen to hold.
    async fn label_until_stable(&self, node: &str, value: &str, expected_uid: &str) -> Result<()> {
        let Some(labelling) = self.labelling.as_ref() else {
            return Ok(());
        };

        let wanted = labelling.keys();
        let updates: Vec<(String, Option<String>)> = wanted
            .iter()
            .map(|key| (key.to_string(), Some(value.to_string())))
            .collect();

        for attempt in 1..=LABEL_APPLY_ATTEMPTS {
            self.rewrite_labels(node, expected_uid, |_, _| updates.clone())
                .await?;

            let mut stable = 0;
            while stable < LABEL_STABILITY_CHECKS {
                tokio::time::sleep(LABEL_CHECK_INTERVAL).await;

                match self.read_labels(node, expected_uid).await {
                    Ok(labels) => {
                        let drifted: Vec<String> = wanted
                            .iter()
                            .filter(|key| labels.get(**key).map(String::as_str) != Some(value))
                            .map(|key| format!("{key}={:?}", labels.get(*key).map(String::as_str)))
                            .collect();

                        if drifted.is_empty() {
                            stable += 1;
                            continue;
                        }

                        warn!(
                            "node {node}: {} after {stable}/{LABEL_STABILITY_CHECKS} stable \
                             observation(s); re-applying (attempt \
                             {attempt}/{LABEL_APPLY_ATTEMPTS})",
                            drifted.join(", ")
                        );
                        break;
                    }
                    Err(err) => {
                        warn!(
                            "node {node}: could not confirm its labels ({err:#}); re-applying \
                             (attempt {attempt}/{LABEL_APPLY_ATTEMPTS})"
                        );
                        break;
                    }
                }
            }

            if stable >= LABEL_STABILITY_CHECKS {
                info!("node {node}: {} are holding", describe_updates(&updates));
                return Ok(());
            }
        }

        anyhow::bail!(
            "node {node} did not hold {} for {LABEL_STABILITY_CHECKS} consecutive checks over \
             {LABEL_APPLY_ATTEMPTS} attempts; something on the node keeps removing them, and \
             workloads would not be selected there",
            describe_updates(&updates)
        )
    }

    async fn read_labels(
        &self,
        node: &str,
        expected_uid: &str,
    ) -> Result<BTreeMap<String, String>> {
        let fetched = self.get(node).await?;
        anyhow::ensure!(
            fetched.metadata.uid.as_deref() == Some(expected_uid),
            "node {node} changed identity while its labels were being verified"
        );
        Ok(fetched.metadata.labels.unwrap_or_default())
    }

    /// Best-effort: a taint left in place only keeps workloads away, which is the
    /// safe direction, so this warns rather than failing an otherwise complete run.
    async fn lift_taints(&self, node: &str, expected_uid: &str) {
        if self.remove_taints.is_empty() {
            return;
        }

        match self.try_lift_taints(node, expected_uid).await {
            Ok(removed) if removed.is_empty() => {
                info!(
                    "node {node}: no matching start-up taint to remove ({})",
                    self.remove_taints.join(", ")
                );
            }
            Ok(removed) => info!(
                "node {node}: removed start-up taint(s) {}",
                removed.join(", ")
            ),
            Err(err) => warn!(
                "node {node}: could not remove start-up taint(s) {} ({err:#}). The node is \
                 labelled, but workloads will stay off it until the taint goes; a later run \
                 retries",
                self.remove_taints.join(", ")
            ),
        }
    }

    /// `.spec.taints` is atomic server-side, so removing one means writing the
    /// whole list back, which would silently drop a taint added meanwhile - in the
    /// direction that admits workloads. Testing the resourceVersion makes that a
    /// rejected write instead.
    async fn try_lift_taints(&self, node: &str, expected_uid: &str) -> Result<Vec<String>> {
        for attempt in 1..=TAINT_PATCH_ATTEMPTS {
            let fetched = self.get(node).await?;
            anyhow::ensure!(
                fetched.metadata.uid.as_deref() == Some(expected_uid),
                "node {node} changed identity before its taints could be removed"
            );
            let version = fetched
                .metadata
                .resource_version
                .clone()
                .unwrap_or_default();
            let current = fetched
                .spec
                .and_then(|spec| spec.taints)
                .unwrap_or_default();
            if current.is_empty() {
                return Ok(Vec::new());
            }

            let (retained, removed) = partition_taints(current, &self.remove_taints);
            if removed.is_empty() {
                return Ok(removed);
            }

            let patch: json_patch::Patch = serde_json::from_value(json!([
                {"op": "test", "path": "/metadata/uid", "value": expected_uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "replace", "path": "/spec/taints", "value": retained},
            ]))
            .context("failed to build the taint patch")?;

            match self
                .api
                .patch(node, &PatchParams::default(), &Patch::Json::<Node>(patch))
                .await
            {
                Ok(_) => return Ok(removed),
                Err(err) if is_precondition_failure(&err) => {
                    info!(
                        "node {node}: its taints changed while they were being lifted \
                         (attempt {attempt}/{TAINT_PATCH_ATTEMPTS}); reading them again"
                    );
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to patch taints on node {node}"))
                }
            }
        }

        anyhow::bail!(
            "gave up lifting taints on node {node} after {TAINT_PATCH_ATTEMPTS} attempts: \
             something keeps changing them concurrently"
        )
    }

    async fn wait_till_ready(&self, node: &str, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        let mut announced = false;

        loop {
            let ready = match self.get(node).await {
                Ok(n) => node_ready_condition(&n).unwrap_or_else(|| "Unknown".to_string()),
                Err(err) => {
                    warn!("node {node}: could not read readiness ({err:#})");
                    "Unknown".to_string()
                }
            };

            if ready == "True" {
                return Ok(());
            }

            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "node {node} did not become Ready within {}s of its Job finishing (last \
                     seen: {ready}); not labelling it, so workloads are not sent to a node whose \
                     CRI runtime may still be restarting",
                    timeout.as_secs()
                );
            }

            if !announced {
                info!(
                    "node {node}: waiting up to {}s for it to report Ready after its Job finished",
                    timeout.as_secs()
                );
                announced = true;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Advisory and never fatal. It lives here rather than in the per-node Job
    /// because it needs `nodes/proxy`.
    async fn warn_on_low_kubelet_timeout(&self, node: &str, threshold: Duration) {
        let probe = tokio::time::timeout(
            KUBELET_PROBE_TIMEOUT,
            self.kubelet_runtime_request_timeout(node),
        );

        let timeout = match probe.await {
            Ok(Ok(Some(value))) => value,
            Ok(Ok(None)) => {
                warn!("node {node}: kubelet /configz did not report runtimeRequestTimeout");
                return;
            }
            Ok(Err(err)) => {
                warn!("node {node}: could not read kubelet runtimeRequestTimeout ({err:#})");
                return;
            }
            Err(_) => {
                warn!(
                    "node {node}: kubelet /configz did not answer within {}s; skipping the \
                     runtimeRequestTimeout warning",
                    KUBELET_PROBE_TIMEOUT.as_secs()
                );
                return;
            }
        };

        let parsed = match humantime::parse_duration(&timeout) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(
                    "node {node}: could not parse kubelet runtimeRequestTimeout {timeout} ({err})"
                );
                return;
            }
        };

        if parsed < threshold {
            warn!(
                "node {node}: kubelet runtimeRequestTimeout is {timeout} ({}s). Pulling a large \
                 image, or converting one, happens during CreateContainer and can exceed it; \
                 consider raising it to at least {}s on these nodes",
                parsed.as_secs(),
                threshold.as_secs()
            );
        } else {
            info!(
                "node {node}: kubelet runtimeRequestTimeout is {timeout} ({}s)",
                parsed.as_secs()
            );
        }
    }

    async fn kubelet_runtime_request_timeout(&self, node: &str) -> Result<Option<String>> {
        let request = Request::new(format!("/api/v1/nodes/{node}/proxy"))
            .get("configz", &GetParams::default())?;
        let configz: serde_json::Value = self
            .client
            .request(request)
            .await
            .with_context(|| format!("failed to query kubelet /configz for node {node}"))?;

        Ok(configz
            .get("kubeletconfig")
            .or_else(|| configz.get("kubeletConfig"))
            .and_then(|config| config.get("runtimeRequestTimeout"))
            .and_then(|value| value.as_str())
            .map(str::to_string))
    }
}

struct GuardedPatchError {
    /// Whether re-reading the node could make the write succeed.
    is_conflict: bool,
    error: anyhow::Error,
}

fn describe_updates(updates: &[(String, Option<String>)]) -> String {
    updates
        .iter()
        .map(|(key, value)| match value {
            Some(value) => format!("{key}={value}"),
            None => format!("{key} (removed)"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `~` and `/` have a meaning of their own in a JSON Pointer (RFC 6901).
fn escape_pointer(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// The apiserver answers a failing JSON Patch `test` with 422 and a genuine write
/// conflict with 409; anything else failed on its merits.
fn is_precondition_failure(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(status) if status.code == 409 || status.code == 422)
}

/// `None` only when the node does not report the field at all (below Kubernetes
/// 1.30, or a kubelet that does not publish it). An empty list is an answer, and
/// the answer is "nothing".
fn served_handlers(node: &Node) -> Option<Vec<String>> {
    Some(
        node.status
            .as_ref()?
            .runtime_handlers
            .as_ref()?
            .iter()
            .filter_map(|handler| handler.name.clone())
            .collect(),
    )
}

#[derive(Debug, PartialEq, Eq)]
enum HandlerVerdict {
    Serving {
        serving: Vec<String>,
        missing: Vec<String>,
    },
    NotServing,
    Unanswerable,
}

/// Any one handler is enough: a caller cannot know a node's architecture, so it
/// names every handler the rollout could install and a node legitimately serves
/// only its own arch's subset. Serving *none* is the verdict worth failing on.
fn handler_verdict(expected: &[String], served: Option<&[String]>) -> HandlerVerdict {
    let Some(served) = served else {
        return HandlerVerdict::Unanswerable;
    };

    let (serving, missing): (Vec<String>, Vec<String>) = expected
        .iter()
        .cloned()
        .partition(|handler| served.contains(handler));

    if serving.is_empty() {
        HandlerVerdict::NotServing
    } else {
        HandlerVerdict::Serving { serving, missing }
    }
}

fn node_ready_condition(node: &Node) -> Option<String> {
    node.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|condition| condition.type_ == "Ready")
        .map(|condition| condition.status.clone())
}

/// Each matcher is `key` (any effect) or `key:effect`. A matcher that matches
/// nothing is not an error: on a re-run the taint is already gone, which is the
/// expected steady state.
fn partition_taints(taints: Vec<Taint>, matchers: &[String]) -> (Vec<Taint>, Vec<String>) {
    let parsed: Vec<(&str, Option<&str>)> = matchers
        .iter()
        .map(|matcher| match matcher.split_once(':') {
            Some((key, effect)) => (key.trim(), Some(effect.trim())),
            None => (matcher.trim(), None),
        })
        .filter(|(key, _)| !key.is_empty())
        .collect();

    let mut retained = Vec::new();
    let mut removed = Vec::new();

    for taint in taints {
        let matched = parsed.iter().any(|(key, effect)| {
            taint.key == *key && effect.map(|e| e == taint.effect).unwrap_or(true)
        });
        if matched {
            removed.push(format!("{}:{}", taint.key, taint.effect));
        } else {
            retained.push(taint);
        }
    }

    (retained, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        NodeCondition, NodeRuntimeHandler, NodeStatus, NodeSystemInfo,
    };

    const SHARED_KEY: &str = "example.com/ready";
    const MARKER_PREFIX: &str = "deployer.example.com";

    fn labelling(instance: Option<&str>) -> NodeLabelling {
        NodeLabelling {
            key: SHARED_KEY.to_string(),
            pending_value: DEFAULT_PENDING_LABEL_VALUE.to_string(),
            instance: Some(InstanceMarker::new(MARKER_PREFIX, instance)),
        }
    }

    fn marker(instance: Option<&str>) -> String {
        InstanceMarker::new(MARKER_PREFIX, instance)
            .key()
            .to_string()
    }

    fn node_serving(handlers: &[&str]) -> Node {
        Node {
            status: Some(NodeStatus {
                runtime_handlers: Some(
                    handlers
                        .iter()
                        .map(|name| NodeRuntimeHandler {
                            name: Some(name.to_string()),
                            features: None,
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn expected(handlers: &[&str]) -> Vec<String> {
        handlers.iter().map(|h| h.to_string()).collect()
    }

    #[test]
    fn one_expected_handler_is_enough() {
        let node = node_serving(&["runc", "special"]);
        assert_eq!(
            handler_verdict(
                &expected(&["special", "special-cc"]),
                served_handlers(&node).as_deref()
            ),
            HandlerVerdict::Serving {
                serving: vec!["special".to_string()],
                missing: vec!["special-cc".to_string()],
            }
        );
    }

    #[test]
    fn no_expected_handler_is_a_runtime_that_ignored_us() {
        let node = node_serving(&["runc"]);
        assert_eq!(
            handler_verdict(&expected(&["special"]), served_handlers(&node).as_deref()),
            HandlerVerdict::NotServing
        );
    }

    #[test]
    fn a_node_that_cannot_answer_never_fails_a_run() {
        assert_eq!(served_handlers(&Node::default()), None);
        assert_eq!(
            handler_verdict(&expected(&["special"]), None),
            HandlerVerdict::Unanswerable
        );
    }

    #[test]
    fn an_empty_list_is_an_answer() {
        assert_eq!(served_handlers(&node_serving(&[])), Some(Vec::new()));
        assert_eq!(
            handler_verdict(
                &expected(&["special"]),
                served_handlers(&node_serving(&[])).as_deref()
            ),
            HandlerVerdict::NotServing
        );
    }

    #[test]
    fn an_instance_is_named_after_its_suffix() {
        assert_eq!(marker(None), "deployer.example.com/default");
        assert_eq!(marker(Some("dev")), "deployer.example.com/dev");
    }

    #[test]
    fn a_marker_prefix_is_normalized() {
        assert_eq!(
            InstanceMarker::new("deployer.example.com/", Some("dev")).key(),
            "deployer.example.com/dev"
        );
        assert_eq!(
            InstanceMarker::new(MARKER_PREFIX, Some("  ")).key(),
            "deployer.example.com/default"
        );
    }

    #[test]
    fn only_keys_under_the_prefix_are_markers() {
        let instance = InstanceMarker::new(MARKER_PREFIX, None);

        assert!(instance.is_marker("deployer.example.com/prod"));
        assert!(!instance.is_marker("deployer.example.com"));
        assert!(!instance.is_marker("deployer.example.com.evil/prod"));
        assert!(!instance.is_marker("kubernetes.io/hostname"));
    }

    #[test]
    fn only_another_instances_mark_counts() {
        let labelling = labelling(Some("dev"));
        let ours = marker(Some("dev"));
        let mark = |keys: &[&str]| -> BTreeMap<String, String> {
            keys.iter()
                .map(|key| (key.to_string(), "true".to_string()))
                .collect()
        };

        assert_eq!(
            shared_label_after(&mark(&[&ours]), &labelling),
            SharedLabel::Remove
        );
        assert_eq!(
            shared_label_after(
                &mark(&[&ours, SHARED_KEY, "kubernetes.io/hostname"]),
                &labelling
            ),
            SharedLabel::Remove,
            "neither the shared label nor an unrelated one is another instance's mark"
        );
        assert_eq!(
            shared_label_after(&mark(&[&ours, &marker(None)]), &labelling),
            SharedLabel::Keep
        );
        assert_eq!(
            shared_label_after(&mark(&[&marker(Some("prod"))]), &labelling),
            SharedLabel::Keep
        );
    }

    #[test]
    fn a_single_instance_owns_the_shared_label_outright() {
        let labelling = NodeLabelling {
            key: SHARED_KEY.to_string(),
            pending_value: DEFAULT_PENDING_LABEL_VALUE.to_string(),
            instance: None,
        };
        let labels = BTreeMap::from([
            (SHARED_KEY.to_string(), "true".to_string()),
            (marker(Some("prod")), "true".to_string()),
        ]);

        assert_eq!(shared_label_after(&labels, &labelling), SharedLabel::Remove);
        assert_eq!(labelling.keys(), vec![SHARED_KEY]);
    }

    #[test]
    fn unfinished_instances_hold_the_key_without_the_promise() {
        let labelling = labelling(Some("dev"));
        let ours = marker(Some("dev"));
        let labels = BTreeMap::from([
            (ours, "true".to_string()),
            (marker(None), DEFAULT_PENDING_LABEL_VALUE.to_string()),
        ]);

        assert_eq!(shared_label_after(&labels, &labelling), SharedLabel::Demote);

        let labels = BTreeMap::from([
            (marker(None), DEFAULT_PENDING_LABEL_VALUE.to_string()),
            (
                marker(Some("prod")),
                DEFAULT_PENDING_LABEL_VALUE.to_string(),
            ),
        ]);

        assert_eq!(shared_label_after(&labels, &labelling), SharedLabel::Demote);
    }

    /// Labels are read in key order, so "default" here is read before "prod".
    #[test]
    fn a_serving_instance_outweighs_unfinished_ones() {
        let labelling = labelling(Some("dev"));
        let labels = BTreeMap::from([
            (marker(None), DEFAULT_PENDING_LABEL_VALUE.to_string()),
            (marker(Some("prod")), "true".to_string()),
        ]);

        assert_eq!(shared_label_after(&labels, &labelling), SharedLabel::Keep);
    }

    #[test]
    fn a_custom_pending_value_is_still_a_claim() {
        let labelling = NodeLabelling {
            key: SHARED_KEY.to_string(),
            pending_value: "installing".to_string(),
            instance: Some(InstanceMarker::new(MARKER_PREFIX, Some("dev"))),
        };
        let labels = BTreeMap::from([(marker(None), "installing".to_string())]);

        assert_eq!(shared_label_after(&labels, &labelling), SharedLabel::Demote);
    }

    #[test]
    fn a_label_key_is_escaped_for_a_json_pointer() {
        assert_eq!(escape_pointer(SHARED_KEY), "example.com~1ready");
        assert_eq!(escape_pointer("a~b/c"), "a~0b~1c");
    }

    #[test]
    fn only_conflicts_are_retried() {
        let status = |code: u16| {
            kube::Error::Api(kube::error::ErrorResponse {
                status: String::new(),
                message: String::new(),
                reason: String::new(),
                code,
            })
        };

        assert!(is_precondition_failure(&status(409)));
        assert!(is_precondition_failure(&status(422)));
        assert!(!is_precondition_failure(&status(403)));
        assert!(!is_precondition_failure(&status(404)));
    }

    fn taint(key: &str, effect: &str) -> Taint {
        Taint {
            key: key.to_string(),
            effect: effect.to_string(),
            value: None,
            time_added: None,
        }
    }

    fn keys(taints: &[Taint]) -> Vec<(String, String)> {
        taints
            .iter()
            .map(|t| (t.key.clone(), t.effect.clone()))
            .collect()
    }

    #[test]
    fn taint_matchers_respect_the_effect() {
        let taints = vec![
            taint("example.com/startup", "NoSchedule"),
            taint("example.com/startup", "NoExecute"),
            taint("other", "NoSchedule"),
        ];

        let (retained, removed) =
            partition_taints(taints.clone(), &["example.com/startup".to_string()]);
        assert_eq!(keys(&retained), vec![("other".into(), "NoSchedule".into())]);
        assert_eq!(removed.len(), 2);

        let (retained, removed) =
            partition_taints(taints, &["example.com/startup:NoExecute".to_string()]);
        assert_eq!(removed, vec!["example.com/startup:NoExecute".to_string()]);
        assert_eq!(keys(&retained).len(), 2);
    }

    #[test]
    fn unmatched_matchers_change_nothing() {
        let taints = vec![taint("other", "NoSchedule")];
        let (retained, removed) = partition_taints(taints, &["example.com/startup".to_string()]);
        assert_eq!(keys(&retained), vec![("other".into(), "NoSchedule".into())]);
        assert!(removed.is_empty());
    }

    #[test]
    fn facts_are_read_off_the_node() {
        let node = Node {
            metadata: kube::core::ObjectMeta {
                name: Some("node-1".to_string()),
                ..Default::default()
            },
            status: Some(NodeStatus {
                node_info: Some(NodeSystemInfo {
                    container_runtime_version: "containerd://2.1.5".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let facts = NodeFacts::from_node(&node);
        assert_eq!(facts.name, "node-1");
        assert_eq!(
            facts.container_runtime_version.as_deref(),
            Some("containerd://2.1.5")
        );
    }

    #[test]
    fn missing_facts_are_absent_not_empty() {
        let node = Node {
            metadata: kube::core::ObjectMeta {
                name: Some("node-2".to_string()),
                ..Default::default()
            },
            status: Some(NodeStatus {
                node_info: Some(NodeSystemInfo {
                    container_runtime_version: String::new(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let facts = NodeFacts::from_node(&node);
        assert!(facts.container_runtime_version.is_none());
    }

    #[test]
    fn ready_condition_is_picked_by_type() {
        let node = Node {
            status: Some(NodeStatus {
                conditions: Some(vec![
                    NodeCondition {
                        type_: "MemoryPressure".to_string(),
                        status: "False".to_string(),
                        ..Default::default()
                    },
                    NodeCondition {
                        type_: "Ready".to_string(),
                        status: "True".to_string(),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(node_ready_condition(&node).as_deref(), Some("True"));
        assert_eq!(node_ready_condition(&Node::default()), None);
    }
}
