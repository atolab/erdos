use futures::{future, stream::SplitSink};
use futures_util::sink::SinkExt;
use std::sync::{Arc};
// use tokio::{
//     self,
//     net::TcpStream,
//     sync::{
//         mpsc::{self, UnboundedReceiver, UnboundedSender},
//         Mutex,
//     },
// };
// use tokio_util::codec::Framed;

// Replacing tokio with async_std equivalents
// use async_std::sync::Arc;
use async_std::channel::{self, Sender, Receiver};
use async_std::sync::Mutex;
use async_std::net::TcpStream;
use futures_codec::Framed;

use crate::communication::{
    CommunicationError, ControlMessage, ControlMessageCodec, ControlMessageHandler,
    InterProcessMessage, MessageCodec,
};
use crate::node::NodeId;
use crate::scheduler::endpoints_manager::ChannelsToSenders;

#[allow(dead_code)]
/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// node which may result in congestion.
pub(crate) struct DataSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: Receiver<InterProcessMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: Sender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: Receiver<ControlMessage>,
}

impl DataSender {
    pub(crate) async fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = channel::unbounded();
        // Add entry in the shared state map.
        channels_to_senders.lock().await.add_sender(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = channel::unbounded();
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
            .await
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx?
        loop {
            match self.rx.recv().await {
                Ok(msg) => {
                    if let Err(e) = self.sink.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }
                }
                Err(_) => return Err(CommunicationError::Disconnected),
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
            .map(|mut sender| async_std::task::spawn(async move { sender.run().await })),
    )
    .await;
    Ok(())
}

#[allow(dead_code)]
/// Listens for control messages on a `tokio::sync::mpsc` channel, and sends received messages on the network.
pub(crate) struct ControlSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: Receiver<ControlMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: Sender<ControlMessage>,
    /// Channel receiver for control messages intended for this `ControlSender`.
    control_rx: Receiver<ControlMessage>,
}

impl ControlSender {
    pub(crate) fn new(
        node_id: NodeId,
        sink: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up channel to other node.
        let (tx, rx) = channel::unbounded();
        control_handler.add_channel_to_node(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = channel::unbounded();
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
            .await
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx
        loop {
            match self.rx.recv().await {
                Ok(msg) => {
                    if let Err(e) = self.sink.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }
                }
                Err(_) => {
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
