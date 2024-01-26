use crate::{
    operations::{Cordoning, Drain, Get, Label, List, PluginResult},
    resources::{
        error::Error,
        utils,
        utils::{print_table, CreateRow, CreateRows, GetHeaderRow, OutputFormat},
        NodeId,
    },
    rest_wrapper::RestClient,
};
use async_trait::async_trait;
use openapi::models::CordonDrainState;
use prettytable::{Cell, Row};
use serde::Serialize;
use std::time;
use strum_macros::{AsRefStr, Display, EnumString};
use tokio::time::Duration;

#[derive(AsRefStr, EnumString, Display)]
enum NodeCordonDrainState {
    Cordoned,
    Draining,
    Drained,
}

#[derive(Debug, Clone, clap::Args)]
/// Arguments used when getting a node.
pub struct GetNodeArgs {
    /// Id of the node
    node_id: NodeId,
}

impl GetNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }
}

/// Nodes resource.
#[derive(clap::Args, Debug)]
pub struct Nodes {}

// CreateRows being trait for Node would create the rows from the list of
// Nodes returned from REST call.
impl CreateRow for openapi::models::Node {
    fn row(&self) -> Row {
        let spec = self.spec.clone().unwrap_or_default();
        // In case the state is not coming as filled, either due to node offline, fill in
        // spec data and mark the status as Unknown.
        let state = self.state.clone().unwrap_or(openapi::models::NodeState {
            id: spec.id,
            grpc_endpoint: spec.grpc_endpoint,
            status: openapi::models::NodeStatus::Unknown,
            node_nqn: spec.node_nqn,
        });
        let statuses = match spec.cordondrainstate {
            None => format!("{:?}", state.status),
            Some(CordonDrainState::cordonedstate(_)) => {
                format!("{:?}, {}", state.status, NodeCordonDrainState::Cordoned)
            }
            Some(CordonDrainState::drainingstate(_)) => {
                format!(
                    "{:?}, {}, {}",
                    state.status,
                    NodeCordonDrainState::Cordoned,
                    NodeCordonDrainState::Draining
                )
            }
            Some(CordonDrainState::drainedstate(_)) => {
                format!(
                    "{:?}, {}, {}",
                    state.status,
                    NodeCordonDrainState::Cordoned,
                    NodeCordonDrainState::Drained
                )
            }
        };
        row![self.id, state.grpc_endpoint, statuses]
    }
}

// GetHeaderRow being trait for Node would return the Header Row for
// Node.
impl GetHeaderRow for openapi::models::Node {
    fn get_header_row(&self) -> Row {
        (*utils::NODE_HEADERS).clone()
    }
}

#[async_trait(?Send)]
impl List for Nodes {
    async fn list(output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_nodes(None).await {
            Ok(nodes) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, nodes.into_body());
            }
            Err(e) => {
                return Err(Error::ListNodesError { source: e });
            }
        }
        Ok(())
    }
}

/// Node resource.
#[derive(clap::Args, Debug)]
pub struct Node {}

#[async_trait(?Send)]
impl Get for Node {
    type ID = NodeId;
    async fn get(id: &Self::ID, output: &utils::OutputFormat) -> PluginResult {
        match RestClient::client().nodes_api().get_node(id).await {
            Ok(node) => {
                // Print table, json or yaml based on output format.
                utils::print_table(output, node.into_body());
            }
            Err(e) => {
                return Err(Error::GetNodeError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}

/// Get the cordon labels from whichever state.
fn cordon_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainingstate(state) => state.cordonlabels.clone(),
        CordonDrainState::drainedstate(state) => state.cordonlabels.clone(),
    }
}

fn drain_labels_from_state(ds: &CordonDrainState) -> Vec<String> {
    match ds {
        CordonDrainState::cordonedstate(_) => Vec::<String>::new(),
        CordonDrainState::drainingstate(state) => state.drainlabels.clone(),
        CordonDrainState::drainedstate(state) => state.drainlabels.clone(),
    }
}

#[async_trait(?Send)]
impl Cordoning for Node {
    type ID = NodeId;
    async fn cordon(id: &Self::ID, label: &str, output: &OutputFormat) -> PluginResult {
        // is node already cordoned with the label?
        let already_has_cordon_label: bool =
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.into_body();
                    match &node_body.spec {
                        Some(spec) => match &spec.cordondrainstate {
                            Some(ds) => cordon_labels_from_state(ds).contains(&label.to_string()),
                            None => false,
                        },
                        None => {
                            println!("Node {id} is not registered");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
                }
            };
        let result = match already_has_cordon_label {
            false => {
                RestClient::client()
                    .nodes_api()
                    .put_node_cordon(id, label)
                    .await
            }
            true => RestClient::client().nodes_api().get_node(id).await,
        };
        match result {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    println!("Node {id} cordoned successfully")
                }
            },
            Err(e) => {
                return Err(Error::NodeCordonError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }

    async fn uncordon(id: &Self::ID, label: &str, output: &OutputFormat) -> PluginResult {
        match RestClient::client()
            .nodes_api()
            .delete_node_cordon(id, label)
            .await
        {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    let mut cordon_labels: Vec<String> = vec![];
                    let mut drain_labels: Vec<String> = vec![];
                    match node.into_body().spec {
                        Some(spec) => {
                            if let Some(cds) = spec.cordondrainstate {
                                cordon_labels = cordon_labels_from_state(&cds);
                                drain_labels = drain_labels_from_state(&cds);
                            }
                        }
                        /* shouldn't happen */
                        None => {
                            println!("Error: Node {id} has no spec");
                        }
                    }
                    let labels = [cordon_labels, drain_labels].concat();
                    if labels.is_empty() {
                        println!("Node {id} successfully uncordoned");
                    } else {
                        println!(
                            "Cordon label successfully removed. Remaining cordon labels {labels:?}",
                        );
                    }
                }
            },
            Err(e) => {
                return Err(Error::NodeUncordonError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}

/// Display format options for a `Node` object.
#[derive(Debug)]
pub enum NodeDisplayFormat {
    Default,
    CordonLabels,
    Drain,
}

/// The NodeDisplay structure is responsible for controlling the display formatting of Node objects.
/// `#[serde(flatten)]` and `#[serde(skip)]` attributes are used to ensure that when the object is
/// serialised, only the `inner` object is represented.
#[derive(Serialize, Debug)]
pub struct NodeDisplay {
    #[serde(flatten)]
    pub inner: Vec<openapi::models::Node>,
    #[serde(skip)]
    format: NodeDisplayFormat,
}

impl NodeDisplay {
    /// Create a new `NodeDisplay` instance.
    pub(crate) fn new(node: openapi::models::Node, format: NodeDisplayFormat) -> Self {
        let vec: Vec<openapi::models::Node> = vec![node];
        Self { inner: vec, format }
    }
    /// Create a new `NodeDisplay` instance from a vector of nodes.
    pub(crate) fn new_nodes(nodes: Vec<openapi::models::Node>, format: NodeDisplayFormat) -> Self {
        Self {
            inner: nodes,
            format,
        }
    }
    /// Get a list of node labels.
    pub(crate) fn get_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut cordon_labels: Vec<String> = vec![];
        let mut drain_labels: Vec<String> = vec![];

        match &node.spec {
            Some(ns) => match &ns.cordondrainstate {
                Some(ds) => {
                    cordon_labels = cordon_labels_from_state(ds);
                    drain_labels = drain_labels_from_state(ds);
                }
                None => {}
            },
            None => {}
        }
        [cordon_labels, drain_labels].concat()
    }
    /// Get a list of node drain labels.
    pub(crate) fn get_drain_label_list(node: &openapi::models::Node) -> Vec<String> {
        let mut drain_labels: Vec<String> = vec![];
        match &node.spec {
            Some(ns) => match &ns.cordondrainstate {
                Some(ds) => {
                    drain_labels = drain_labels_from_state(ds);
                }
                None => {}
            },
            None => {}
        }
        drain_labels
    }
}

impl CreateRows for NodeDisplay {
    fn create_rows(&self) -> Vec<Row> {
        match self.format {
            NodeDisplayFormat::Default => self.inner.create_rows(),
            NodeDisplayFormat::CordonLabels => {
                let mut rows = vec![];
                for node in self.inner.iter() {
                    let mut row = node.create_rows();
                    let labelstring = NodeDisplay::get_label_list(node).join(", ");
                    // Add the cordon labels to each row.
                    row[0].add_cell(Cell::new(&labelstring));
                    rows.push(row[0].clone());
                }
                rows
            }
            NodeDisplayFormat::Drain => {
                let mut rows = Vec::with_capacity(self.inner.len());
                for node in self.inner.iter() {
                    let mut row = node.row();

                    let drain_status_string = match &node.spec.as_ref().unwrap().cordondrainstate {
                        Some(ds) => match ds {
                            CordonDrainState::cordonedstate(_) => "Not draining",
                            CordonDrainState::drainingstate(_) => "Draining",
                            CordonDrainState::drainedstate(_) => "Drained",
                        },
                        None => "Not draining",
                    };

                    let labelstring = NodeDisplay::get_drain_label_list(node).join(", ");
                    // Add the drain labels to each row.
                    row.add_cell(Cell::new(drain_status_string));
                    row.add_cell(Cell::new(&labelstring));
                    rows.push(row);
                }
                rows
            }
        }
    }
}

/// Print the given vector of nodes in the specified output format.
pub(crate) fn node_display_print(
    nodes: Vec<openapi::models::Node>,
    output: &OutputFormat,
    format: NodeDisplayFormat,
) {
    let node_display = NodeDisplay::new_nodes(nodes, format);
    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            print_table(output, node_display.inner);
        }
        OutputFormat::None => {
            print_table(output, node_display);
        }
    }
}

/// Print the given node in the specified output format.
pub(crate) fn node_display_print_one(
    nodes: openapi::models::Node,
    output: &OutputFormat,
    format: NodeDisplayFormat,
) {
    let node_display = NodeDisplay::new(nodes, format);
    match output {
        OutputFormat::Yaml | OutputFormat::Json => {
            print_table(output, node_display.inner);
        }
        OutputFormat::None => {
            print_table(output, node_display);
        }
    }
}

// Create the header for a `NodeDisplay` object.
impl GetHeaderRow for NodeDisplay {
    fn get_header_row(&self) -> Row {
        let mut header = (*utils::NODE_HEADERS).clone();
        match self.format {
            NodeDisplayFormat::Default => header,
            NodeDisplayFormat::CordonLabels => {
                header.extend(vec!["CORDON LABELS"]);
                header
            }
            NodeDisplayFormat::Drain => {
                header.extend(vec!["DRAIN STATE"]);
                header.extend(vec!["DRAIN LABELS"]);
                header
            }
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct DrainNodeArgs {
    /// Id of the node.
    node_id: NodeId,
    /// Label of the drain.
    label: String,
    #[clap(long)]
    /// Timeout for the drain operation.
    drain_timeout: Option<humantime::Duration>,
}

impl DrainNodeArgs {
    /// Return the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }
    /// Return the drain label.
    pub fn label(&self) -> String {
        self.label.clone()
    }
    /// Return the timeout for the drain operation.
    pub fn drain_timeout(&self) -> Option<humantime::Duration> {
        self.drain_timeout
    }
}

#[async_trait(?Send)]
impl Drain for Node {
    type ID = NodeId;
    async fn drain(
        id: &Self::ID,
        label: String,
        drain_timeout: Option<humantime::Duration>,
        output: &utils::OutputFormat,
    ) -> PluginResult {
        let mut timeout_instant: Option<time::Instant> = None;
        if let Some(dt) = drain_timeout {
            timeout_instant = time::Instant::now().checked_add(dt.into());
        }
        let already_has_drain_label: bool =
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.into_body();
                    match &node_body.spec {
                        Some(spec) => match &spec.cordondrainstate {
                            Some(ds) => drain_labels_from_state(ds).contains(&label),
                            None => false,
                        },
                        None => {
                            println!("Node {id} is not registered");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
                }
            };
        if !already_has_drain_label {
            if let Err(error) = RestClient::client()
                .nodes_api()
                .put_node_drain(id, &label)
                .await
            {
                return Err(Error::PutNodeDrainError {
                    id: id.to_string(),
                    source: error,
                });
            }
        }
        // loop this call until no longer draining
        loop {
            match RestClient::client().nodes_api().get_node(id).await {
                Ok(node) => {
                    let node_body = &node.clone().into_body();
                    match &node_body.spec {
                        Some(spec) => {
                            match &spec.cordondrainstate {
                                Some(ds) => match ds {
                                    CordonDrainState::cordonedstate(_) => {
                                        match output {
                                            OutputFormat::None => {
                                                println!("Node {id} drain has been cancelled");
                                            }
                                            _ => {
                                                // json or yaml
                                                print_table(output, node.into_body());
                                            }
                                        }
                                        break;
                                    }
                                    CordonDrainState::drainingstate(_) => {}
                                    CordonDrainState::drainedstate(_) => {
                                        match output {
                                            OutputFormat::None => {
                                                println!("Node {id} successfully drained");
                                            }
                                            _ => {
                                                // json or yaml
                                                print_table(output, node.into_body());
                                            }
                                        }
                                        break;
                                    }
                                },
                                None => {
                                    match output {
                                        OutputFormat::None => {
                                            println!("Node {id} drain has been cancelled");
                                        }
                                        _ => {
                                            // json or yaml
                                            print_table(output, node.into_body());
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        None => {
                            println!("Node {id} is not registered");
                            break;
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::GetNodeError {
                        id: id.to_string(),
                        source: e,
                    });
                }
            }
            if timeout_instant.is_some() && time::Instant::now() > timeout_instant.unwrap() {
                println!("Node {id} drain command timed out");
                break;
            }
            let sleep = Duration::from_secs(2);
            tokio::time::sleep(sleep).await;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Label for Node {
    type ID = NodeId;
    async fn label(
        id: &Self::ID,
        label: String,
        overwrite: bool,
        output: &utils::OutputFormat,
    ) -> PluginResult {
        let result = match label.chars().last() {
            Some(last_char) => {
                // If the last character is a hyphen the it signifies that the lable needs to be
                // removed.
                if last_char == '-' {
                    RestClient::client()
                        .nodes_api()
                        .delete_node_label(id, &label[.. label.len() - 1])
                        .await
                } else {
                    RestClient::client()
                        .nodes_api()
                        .put_node_label(id, &label, Some(overwrite))
                        .await
                }
            }
            None => {
                return Err(Error::EmptyNodeLabelError { id: id.to_string() });
            }
        };

        match result {
            Ok(node) => match output {
                OutputFormat::Yaml | OutputFormat::Json => {
                    // Print json or yaml based on output format.
                    utils::print_table(output, node.into_body());
                }
                OutputFormat::None => {
                    // In case the output format is not specified, show a success message.
                    println!("Node {id} labelled successfully")
                }
            },
            Err(e) => {
                return Err(Error::NodeLabelError {
                    id: id.to_string(),
                    source: e,
                });
            }
        }
        Ok(())
    }
}
