use futures::future;
use futures_util::stream::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

#[cfg(feature = "zenoh_zerocopy_transport")]
use zenoh::net;

#[cfg(feature = "zenoh_zerocopy_transport")]
static SHM_SIZE: usize = (512 * 1024 * 1024);

use crate::{
    communication::{
        CodecError, CommunicationError, ControlMessage, ControlMessageHandler, InterProcessMessage,
        PusherT,
    },
    dataflow::stream::StreamId,
    node::NodeId,
    scheduler::endpoints_manager::ChannelsToReceivers,
};

/// Listens on a Zenoh Subscriber stream, and pushes messages it receives to operator executors.
#[allow(dead_code)]
#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) struct ZenohShmDataReceiver {
    /// The id of the node the stream is receiving data from.
    node_id: NodeId,
    /// Self node id
    self_node_id: NodeId,
    /// Zenoh Session
    zsession: Arc<net::Session>,
    /// Channel receiver on which new pusher updates are received.
    rx: UnboundedReceiver<(StreamId, Box<dyn PusherT>)>,
    /// Mapping between stream id to [`PusherT`] trait objects.
    /// [`PusherT`] trait objects are used to deserialize and send
    /// messages to operators.
    stream_id_to_pusher: HashMap<StreamId, Box<dyn PusherT>>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

#[cfg(feature = "zenoh_zerocopy_transport")]
impl ZenohShmDataReceiver {
    pub(crate) async fn new(
        node_id: NodeId,
        self_node_id: NodeId,
        zsession: Arc<net::Session>,
        channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state vector.
        channels_to_receivers.lock().await.add_sender(tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_receiver(node_id, control_tx);
        Self {
            node_id,
            self_node_id,
            zsession,
            rx,
            stream_id_to_pusher: HashMap::new(),
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {

        let id = format!("from-{}-to-{}-data", self.node_id, self.self_node_id);

        let mut shm =
            zenoh::net::SharedMemoryManager::new(id, SHM_SIZE).map_err(CommunicationError::from)?;

        //Create the Zenoh subscription
        let sub_info = zenoh::net::SubInfo {
            reliability: zenoh::net::protocol::core::Reliability::Reliable,
            mode: zenoh::net::protocol::core::SubMode::Push,
            period: None,
        };

        let resource_name = format!("/from/{}/to/{}/data", self.node_id, self.self_node_id);
        let zsession = self.zsession.clone();

        let mut subscriber = zsession
            .declare_subscriber(&resource_name.into(), &sub_info)
            .await
            .map_err(CommunicationError::from)?;

        tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;

        // Notify `ControlMessageHandler` that receiver is initialized.
        self.control_tx
            .send(ControlMessage::DataReceiverInitialized(self.node_id))
            .map_err(CommunicationError::from)?;

        let z_sub_stream = subscriber.stream();
        while let Some(zres) = z_sub_stream.next().await {
            //let m = InterProcessMessage::from_sbuf(zres.payload, &mut shm);

            let m = loop {
                match InterProcessMessage::from_sbuf(zres.payload.clone(), &mut shm) {
                    Ok(msg) => break Ok(msg),
                    Err(CodecError::ZenohSharedMemoryError(e)) => {
                        println!(
                            "### Unable to allocate on DataReceiver - Manager: {:?} {}",
                            shm, e
                        );
                        tokio::time::delay_for(tokio::time::Duration::from_millis(500)).await;
                        shm.garbage_collect();
                    }
                    Err(e) => break Err(e),
                }
            };

            match m {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    // Update pushers before we send the message.
                    // Note: we may want to update the pushers less frequently.
                    self.update_pushers();
                    // Send the message.
                    let (metadata, bytes) = match msg {
                        InterProcessMessage::Serialized { metadata, bytes } => (metadata, bytes),
                        InterProcessMessage::Deserialized {
                            metadata: _,
                            data: _,
                        } => unreachable!(),
                    };
                    match self.stream_id_to_pusher.get_mut(&metadata.stream_id) {
                        Some(pusher) => {
                            if let Err(e) = pusher.send_from_bytes(bytes) {
                                return Err(e);
                            }
                        }
                        None => panic!(
                            "Receiver does not have any pushers. \
                                Race condition during data-flow reconfiguration."
                        ),
                    }
                }
                Err(e) => {
                    slog::warn!(
                        crate::get_terminal_logger(),
                        "DataReceiver got error from Zenoh {:?}",
                        e
                    );
                    return Err(CommunicationError::from(e));
                }
            }
        }

        Ok(())
    }

    // TODO: update this method.
    fn update_pushers(&mut self) {
        // Execute while we still have pusher updates.
        while let Ok((stream_id, pusher)) = self.rx.try_recv() {
            self.stream_id_to_pusher.insert(stream_id, pusher);
        }
    }
}

/// Receives TCP messages, and pushes them to operators endpoints.
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) async fn run_receivers(
    mut receivers: Vec<ZenohShmDataReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}

/// Listens on a Zenoh Subscriber stream, and pushes control messages it receives to the node.
#[allow(dead_code)]
#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) struct ZenohShmControlReceiver {
    /// The id of the node the stream is receiving data from.
    node_id: NodeId,
    /// Self node id
    self_node_id: NodeId,
    /// Framed TCP read stream.
    // stream: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
    /// Zenoh Session
    zsession: Arc<net::Session>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

#[cfg(feature = "zenoh_zerocopy_transport")]
impl ZenohShmControlReceiver {
    pub(crate) fn new(
        node_id: NodeId,
        self_node_id: NodeId,
        zsession: Arc<net::Session>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up control channel.
        let (tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        control_handler.add_channel_to_control_receiver(node_id, tx);
        Self {
            node_id,
            // stream,
            self_node_id,
            zsession,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // TODO: update `self.channel_to_handler` for up-to-date mappings.
        // between channels and handlers (e.g. for fault-tolerance).
        // Notify `ControlMessageHandler` that sender is initialized.

        // Create Zenoh subscription

        let sub_info = zenoh::net::SubInfo {
            reliability: zenoh::net::protocol::core::Reliability::Reliable,
            mode: zenoh::net::protocol::core::SubMode::Push,
            period: None,
        };

        let resource_name = format!("/from/{}/to/{}/control", self.node_id, self.self_node_id);

        let zsession = self.zsession.clone();

        let mut subscriber = zsession
            .declare_subscriber(&resource_name.into(), &sub_info)
            .await
            .map_err(CommunicationError::from)?;

        // Needed to solve the Write-before-publisher-discovered issue
        tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;

        self.control_tx
            .send(ControlMessage::ControlReceiverInitialized(self.node_id))
            .map_err(CommunicationError::from)?;

        let z_sub_stream = subscriber.stream();
        while let Some(zres) = z_sub_stream.next().await {
            match ControlMessage::from_rbuf(&zres.payload) {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    self.control_tx
                        .send(msg)
                        .map_err(CommunicationError::from)?;
                }
                Err(e) => return Err(CommunicationError::from(e)),
            }
        }

        Ok(())
    }
}

/// Receives TCP messages, and pushes them to the ControlHandler
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) async fn run_control_receivers(
    mut receivers: Vec<ZenohShmControlReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}
