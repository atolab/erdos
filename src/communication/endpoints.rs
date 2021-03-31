use std::{fmt::Debug, sync::Arc};
// use tokio::sync::mpsc;

use async_std::channel;

use crate::{
    communication::{CommunicationError, InterProcessMessage, Serializable, TryRecvError},
    dataflow::stream::StreamId,
};

/// Endpoint to be used to send messages between operators.
#[derive(Clone)]
pub enum SendEndpoint<D: Clone + Send + Debug> {
    /// Send messages to an operator running in the same process.
    InterThread(channel::Sender<D>),
    /// Send messages to operators running on a different node.
    /// Data is first sended to [`DataSender`](crate::communication::senders::DataSender)
    /// which encodes and sends the message on a TCP stream.
    InterProcess(StreamId, channel::Sender<InterProcessMessage>),
}

/// Zero-copy implementation of the endpoint.
/// Because we [`Arc`], the message isn't copied when sent between endpoints within the node.
impl<D: 'static + Serializable + Send + Sync + Debug> SendEndpoint<Arc<D>> {
    pub async fn send(&mut self, msg: Arc<D>) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).await.map_err(CommunicationError::from),
            Self::InterProcess(stream_id, sender) => sender
                .send(InterProcessMessage::new_deserialized(msg, *stream_id))
                .await
                .map_err(CommunicationError::from),
        }
    }
}

/// Endpoint to be used to receive messages.
pub enum RecvEndpoint<D: Clone + Send + Debug> {
    InterThread(channel::Receiver<D>),
}

impl<D: Clone + Send + Debug> RecvEndpoint<D> {
    /// Aync read of a new message.
    pub async fn read(&mut self) -> Result<D, CommunicationError> {
        match self {
            Self::InterThread(receiver) => receiver
                .recv()
                .await
                .map_err(|_| CommunicationError::Disconnected),
        }
    }

    /// Non-blocking read of a new message. Returns `TryRecvError::Empty` if no message is available.
    pub fn try_read(&mut self) -> Result<D, TryRecvError> {
        match self {
            Self::InterThread(receiver) => receiver.try_recv().map_err(TryRecvError::from),
        }
    }
}
