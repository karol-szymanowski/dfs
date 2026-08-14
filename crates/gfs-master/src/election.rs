use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[async_trait]
pub trait LeaderElector: Send + Sync {
    async fn is_leader(&self) -> bool;
    async fn run_election_loop(&self, token: CancellationToken) -> anyhow::Result<()>;
}

/// Static leader implementation for local and integration tests without Kubernetes.
pub struct StaticLeader {
    is_leader: AtomicBool,
}

impl StaticLeader {
    pub fn new(initial_leader: bool) -> Self {
        Self {
            is_leader: AtomicBool::new(initial_leader),
        }
    }

    pub fn set_leader(&self, leader: bool) {
        self.is_leader.store(leader, Ordering::SeqCst);
    }
}

#[async_trait]
impl LeaderElector for StaticLeader {
    async fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    async fn run_election_loop(&self, token: CancellationToken) -> anyhow::Result<()> {
        token.cancelled().await;
        Ok(())
    }
}

/// Kubernetes Lease-based leader elector.
pub struct KubeLeaseElector {
    pub lease_name: String,
    pub namespace: String,
    pub holder_id: String,
    is_leader: Arc<AtomicBool>,
}

impl KubeLeaseElector {
    pub fn new(lease_name: String, namespace: String, holder_id: String) -> Self {
        Self {
            lease_name,
            namespace,
            holder_id,
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl LeaderElector for KubeLeaseElector {
    async fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    async fn run_election_loop(&self, token: CancellationToken) -> anyhow::Result<()> {
        info!(
            "Starting KubeLease election loop for lease {} in namespace {} as {}",
            self.lease_name, self.namespace, self.holder_id
        );

        let client = match kube::Client::try_default().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create Kube client: {}", e);
                return Err(e.into());
            }
        };

        let leases: kube::Api<k8s_openapi::api::coordination::v1::Lease> =
            kube::Api::namespaced(client, &self.namespace);

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        while !token.is_cancelled() {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = interval.tick() => {
                    match leases.get_opt(&self.lease_name).await {
                        Ok(Some(lease)) => {
                            let holder = lease.spec.as_ref().and_then(|s| s.holder_identity.as_ref());
                            if holder == Some(&self.holder_id) {
                                self.is_leader.store(true, Ordering::SeqCst);
                            } else {
                                self.is_leader.store(false, Ordering::SeqCst);
                            }
                        }
                        Ok(None) => {
                            warn!("Lease {} not found, attempting creation", self.lease_name);
                            self.is_leader.store(false, Ordering::SeqCst);
                        }
                        Err(e) => {
                            error!("Error querying lease {}: {}", self.lease_name, e);
                            self.is_leader.store(false, Ordering::SeqCst);
                        }
                    }
                }
            }
        }

        self.is_leader.store(false, Ordering::SeqCst);
        info!("KubeLease election loop stepped down");
        Ok(())
    }
}
