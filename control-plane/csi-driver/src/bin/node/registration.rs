use csi_driver::client::RestApiClient;
use snafu::Snafu;
use std::{collections::HashMap, time::Duration};
use tokio::task::JoinError;
use tracing::trace;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub(crate) enum RegistrationError {
    TokioTaskWait { source: JoinError },
}

/// Default registration interval.
const DEFAULT_REGISTRATION_INTERVAL: Duration = Duration::from_secs(60 * 5);
/// Default registration interval on error.
const DEFAULT_REGISTRATION_INTERVAL_ON_ERROR: Duration = Duration::from_secs(30);

pub(crate) async fn run_registration_loop(
    id: String,
    endpoint: String,
    labels: Option<HashMap<String, String>>,
) -> Result<(), RegistrationError> {
    let client = RestApiClient::get_client();

    tokio::spawn(async move {
        loop {
            let interval_duration = match client.register_app_node(&id, &endpoint, &labels).await {
                Ok(_) => {
                    // If register_app_node is successful, set the interval to
                    // DEFAULT_REGISTRATION_INTERVAL.
                    DEFAULT_REGISTRATION_INTERVAL
                }
                Err(e) => {
                    trace!("Failed to register app node: {:?}", e);
                    // If register_app_node fails, set the interval to
                    // DEFAULT_REGISTRATION_INTERVAL_ON_ERROR.
                    DEFAULT_REGISTRATION_INTERVAL_ON_ERROR
                }
            };

            // Sleep for the interval duration.
            tokio::time::sleep(interval_duration).await;
        }
    })
    .await
    .map_err(|error| RegistrationError::TokioTaskWait { source: error })?;

    Ok(())
}
