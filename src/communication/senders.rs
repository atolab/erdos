use futures::future;

use std::sync::Arc;
use tokio::{
    self,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};

#[cfg(feature = "tcp_transport")]
use futures_util::sink::SinkExt;
#[cfg(feature = "tcp_transport")]
use futures::stream::SplitSink;
#[cfg(feature = "tcp_transport")]
use tokio::net::TcpStream;
#[cfg(feature = "tcp_transport")]
use tokio_util::codec::Framed;

#[cfg(feature = "tcp_transport")]
use crate::communication::{ControlMessageCodec, MessageCodec};

#[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
use zenoh::net;

use crate::communication::{
    CommunicationError, ControlMessage, ControlMessageHandler,
    InterProcessMessage,
};
use crate::node::NodeId;
use crate::scheduler::endpoints_manager::ChannelsToSenders;

#[allow(dead_code)]
/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// node which may result in congestion.
#[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
pub(crate) struct DataSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Self node id
    self_node_id: NodeId,
    /// Zenoh Session
    zsession : Arc<net::Session>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<InterProcessMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}


#[allow(dead_code)]
#[cfg(feature = "tcp_transport")]
pub(crate) struct DataSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<InterProcessMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

impl DataSender {

    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub(crate) async fn new(
        node_id: NodeId,
        self_node_id: NodeId,
        zsession : Arc<net::Session>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state map.
        channels_to_senders.lock().await.add_sender(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_sender(node_id, control_tx);
        Self {
            node_id,
            self_node_id,
            // sink,
            zsession,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    #[cfg(feature = "tcp_transport")]
    pub(crate) async fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state map.
        channels_to_senders.lock().await.add_sender(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_sender(node_id, control_tx);
        Self {
            node_id,
            sink,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify [`ControlMessageHandler`] that sender is initialized.
        self.control_tx
            .send(ControlMessage::DataSenderInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx?
        loop {
            match self.rx.recv().await {
                Some(msg) => {
                    #[cfg(feature = "tcp_transport")]
                    {
                        if let Err(e) = self.sink.send(msg.clone()).await.map_err(CommunicationError::from) {
                            return Err(e);
                        }
                    }

                    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
                    {
                        // Sending over Zenoh-net

                        let res_name = format!("/from/{}/to/{}/data",self.self_node_id, self.node_id);

                        let reskey =
                            zenoh::net::protocol::core::ResKey::RId(
                                self.zsession
                                    .declare_resource(&res_name.into())
                                    .await
                                    .map_err(CommunicationError::from)?
                            );
                        let _publ = self.zsession.declare_publisher(&reskey).await.map_err(CommunicationError::from)?;

                        let rbf: zenoh::net::RBuf = msg.into_rbuf().map_err(CommunicationError::from)?;

                        if let Err(e) = self
                            .zsession
                            .write_ext(
                                &reskey,
                                rbf,
                                zenoh::net::encoding::DEFAULT,
                                zenoh::net::data_kind::DEFAULT,
                                zenoh::net::protocol::core::CongestionControl::Block,
                            )
                            .await
                            .map_err(CommunicationError::from)
                        {
                            return Err(e);
                        }
                        //
                    }

                }
                None => return Err(CommunicationError::Disconnected),
            }
        }
    }
}

/// Sends messages received from operator executors to other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `InterProcessMessages` messages, which it
/// forwards on the TCP stream.
pub(crate) async fn run_senders(senders: Vec<DataSender>) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(
        senders
            .into_iter()
            .map(|mut sender| tokio::spawn(async move { sender.run().await })),
    )
    .await;
    Ok(())
}

#[allow(dead_code)]
/// Listens for control messages on a `tokio::sync::mpsc` channel, and sends received messages on the network.
#[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
pub(crate) struct ControlSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Self node id
    self_node_id: NodeId,
    /// Zenoh Session
    zsession : Arc<net::Session>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<ControlMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Channel receiver for control messages intended for this `ControlSender`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

#[allow(dead_code)]
#[cfg(feature = "tcp_transport")]
pub(crate) struct ControlSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<ControlMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Channel receiver for control messages intended for this `ControlSender`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

impl ControlSender {

    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub(crate) fn new(
        node_id: NodeId,
        self_node_id: NodeId,
        zsession : Arc<net::Session>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up channel to other node.
        let (tx, rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_node(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_control_sender(node_id, control_tx);
        Self {
            node_id,
            self_node_id,
            // sink,
            zsession,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    #[cfg(feature = "tcp_transport")]
    pub(crate) fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up channel to other node.
        let (tx, rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_node(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_control_sender(node_id, control_tx);
        Self {
            node_id,
            sink,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify `ControlMessageHandler` that sender is initialized.
        self.control_tx
            .send(ControlMessage::ControlSenderInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx
        loop {
            match self.rx.recv().await {
                Some(msg) => {

                    #[cfg(feature = "tcp_transport")]
                    {
                        if let Err(e) = self.sink.send(msg.clone()).await.map_err(CommunicationError::from) {
                            return Err(e);
                        }
                    }

                    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
                    {
                        // Sending on Zenoh
                        let res_name = format!("/from/{}/to/{}/control",self.self_node_id, self.node_id);

                        let reskey =
                            zenoh::net::protocol::core::ResKey::RId(
                                self.zsession
                                    .declare_resource(&res_name.into())
                                    .await
                                    .map_err(CommunicationError::from)?
                            );
                        let _publ = self.zsession.declare_publisher(&reskey).await.map_err(CommunicationError::from)?;
                        let rbf: zenoh::net::RBuf = msg.into_rbuf().map_err(CommunicationError::from)?;

                        if let Err(e) = self
                            .zsession
                            .write_ext(
                                &reskey,
                                rbf,
                                zenoh::net::encoding::DEFAULT,
                                zenoh::net::data_kind::DEFAULT,
                                zenoh::net::protocol::core::CongestionControl::Block,
                            )
                            .await
                            .map_err(CommunicationError::from)
                        {
                            return Err(e);
                        }
                        //
                    }



                }
                None => {
                    return Err(CommunicationError::Disconnected);
                }
            }
        }
    }
}

/// Sends messages received from the control handler other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `ControlMessage`s, which it
/// forwards on the TCP stream.
pub(crate) async fn run_control_senders(
    mut senders: Vec<ControlSender>,
) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(senders.iter_mut().map(|sender| sender.run())).await;
    Ok(())
}
