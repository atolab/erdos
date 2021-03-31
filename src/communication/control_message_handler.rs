use std::collections::HashMap;

use slog::{self, Logger};
// use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use async_std::channel::{self, Sender, Receiver};

use crate::node::NodeId;

use super::{CommunicationError, ControlMessage};

// TODO: update `channels_to_nodes` for fault tolerance in case nodes to go down.
pub struct ControlMessageHandler {
    /// Logger for error messages.
    logger: Logger,
    /// Sender to clone so other tasks can send messages to `self.rx`.
    tx: Sender<ControlMessage>,
    /// Receiver for all `ControlMessage`s
    rx: Receiver<ControlMessage>,
    channels_to_control_senders: HashMap<NodeId, Sender<ControlMessage>>,
    channels_to_control_receivers: HashMap<NodeId, Sender<ControlMessage>>,
    channels_to_data_senders: HashMap<NodeId, Sender<ControlMessage>>,
    channels_to_data_receivers: HashMap<NodeId, Sender<ControlMessage>>,
    channels_to_nodes: HashMap<NodeId, Sender<ControlMessage>>,
}

#[allow(dead_code)]
impl ControlMessageHandler {
    pub fn new(logger: Logger) -> Self {
        let (tx, rx) = channel::unbounded();
        Self {
            logger,
            tx,
            rx,
            channels_to_control_senders: HashMap::new(),
            channels_to_control_receivers: HashMap::new(),
            channels_to_data_senders: HashMap::new(),
            channels_to_data_receivers: HashMap::new(),
            channels_to_nodes: HashMap::new(),
        }
    }

    pub fn add_channel_to_control_sender(
        &mut self,
        node_id: NodeId,
        tx: Sender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_control_senders.insert(node_id, tx) {
            slog::error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to control sender for node {}",
                node_id
            );
        }
    }

    pub async fn send_to_control_sender(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_control_senders.get_mut(&node_id) {
            Some(tx) => tx.send(msg).await.map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub async fn broadcast_to_control_senders(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_control_senders.values_mut() {
            tx.send(msg.clone()).await.map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_control_receiver(
        &mut self,
        node_id: NodeId,
        tx: Sender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_control_receivers.insert(node_id, tx) {
            slog::error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to control receiver for node {}",
                node_id
            );
        }
    }

    pub async fn send_to_control_receiver(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_control_receivers.get_mut(&node_id) {
            Some(tx) => tx.send(msg).await.map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub async fn broadcast_to_control_receivers(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_control_receivers.values_mut() {
            tx.send(msg.clone()).await.map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_data_sender(
        &mut self,
        node_id: NodeId,
        tx: Sender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_data_senders.insert(node_id, tx) {
            slog::error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to data sender for node {}",
                node_id
            );
        }
    }

    pub async fn send_to_data_sender(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_data_senders.get_mut(&node_id) {
            Some(tx) => tx.send(msg).await.map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub async fn broadcast_to_data_senders(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_data_senders.values_mut() {
            tx.send(msg.clone()).await.map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_data_receiver(
        &mut self,
        node_id: NodeId,
        tx: Sender<ControlMessage>,
    ) {
        if let Some(_) = self.channels_to_data_receivers.insert(node_id, tx) {
            slog::error!(
                self.logger,
                "ControlMessageHandler: overwrote channel to data receiver for node {}",
                node_id
            );
        }
    }

    pub async fn send_to_data_receiver(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_data_receivers.get_mut(&node_id) {
            Some(tx) => tx.send(msg).await.map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub async fn broadcast_to_data_receivers(
        &mut self,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        for tx in self.channels_to_data_receivers.values_mut() {
            tx.send(msg.clone()).await.map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn add_channel_to_node(&mut self, node_id: NodeId, tx: Sender<ControlMessage>) {
        self.channels_to_nodes.insert(node_id, tx);
    }

    pub async fn send_to_node(
        &mut self,
        node_id: NodeId,
        msg: ControlMessage,
    ) -> Result<(), CommunicationError> {
        match self.channels_to_nodes.get_mut(&node_id) {
            Some(tx) => tx.send(msg).await.map_err(CommunicationError::from),
            None => Err(CommunicationError::Disconnected),
        }
    }

    pub async fn broadcast_to_nodes(&mut self, msg: ControlMessage) -> Result<(), CommunicationError> {
        for tx in self.channels_to_nodes.values_mut() {
            tx.send(msg.clone()).await.map_err(CommunicationError::from)?;
        }
        Ok(())
    }

    pub fn get_channel_to_handler(&self) -> Sender<ControlMessage> {
        self.tx.clone()
    }

    pub async fn read(&mut self) -> Result<ControlMessage, CommunicationError> {
        self.rx.recv().await.map_err(|_| CommunicationError::Disconnected)
    }

    // TODO: try to implement this via a generic
    /// Reads messages until a `ControlMessage::AllOperatorsInitializedOnNode` is received
    /// without consuming any other messages types.
    /// Note: this may affect message order.
    pub async fn read_all_operators_initialized_on_node_msg(
        &mut self,
    ) -> Result<NodeId, CommunicationError> {
        let mut read_msgs = Vec::new();
        let mut result = None;
        while result.is_none() {
            match self.read().await {
                Ok(ControlMessage::AllOperatorsInitializedOnNode(node_id)) => {
                    result = Some(Ok(node_id))
                }
                Ok(msg) => read_msgs.push(msg),
                Err(e) => result = Some(Err(e)),
            };
        }
        // Re-enqueue read messages.
        for msg in read_msgs {
            self.tx.send(msg).await.map_err(CommunicationError::from)?;
        }
        result.unwrap()
    }

    pub async fn read_sender_or_receiver_initialized(
        &mut self,
    ) -> Result<ControlMessage, CommunicationError> {
        let mut read_msgs = Vec::new();
        let mut result = None;
        while result.is_none() {
            match self.read().await {
                Ok(control_msg) => match control_msg.clone() {
                    ControlMessage::ControlSenderInitialized(_)
                    | ControlMessage::ControlReceiverInitialized(_)
                    | ControlMessage::DataSenderInitialized(_)
                    | ControlMessage::DataReceiverInitialized(_) => result = Some(Ok(control_msg)),
                    _ => read_msgs.push(control_msg),
                },
                Err(e) => result = Some(Err(e)),
            };
        }
        // Re-enqueue read messages.
        for msg in read_msgs {
            self.tx.send(msg).await.map_err(CommunicationError::from)?;
        }
        result.unwrap()
    }
}
