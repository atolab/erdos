use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{dataflow::stream::StreamId, node::NodeId, OperatorId};
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use futures::future;
use serde::{Deserialize, Serialize};
use slog;
use std::boxed::Box;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    prelude::*,
    time::delay_for,
};

// Private submodules
mod control_message_codec;
mod control_message_handler;
mod endpoints;
mod errors;
mod message_codec;
mod serializable;

// Crate-wide visible submodules
pub(crate) mod pusher;

#[cfg(feature = "tcp_transport")]
pub(crate) mod receivers;

#[cfg(feature = "tcp_transport")]
pub(crate) mod senders;

#[cfg(feature = "zenoh_transport")]
pub(crate) mod zenoh_senders;

#[cfg(feature = "zenoh_transport")]
pub(crate) mod zenoh_receivers;

#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) mod zenoh_shm_senders;

#[cfg(feature = "zenoh_zerocopy_transport")]
pub(crate) mod zenoh_shm_receivers;

// Private imports
use serializable::Serializable;

// Module-wide exports
#[cfg(feature = "tcp_transport")]
pub(crate) use control_message_codec::ControlMessageCodec;
#[cfg(feature = "tcp_transport")]
pub(crate) use message_codec::MessageCodec;

pub(crate) use control_message_handler::ControlMessageHandler;
pub(crate) use errors::{CodecError, CommunicationError, TryRecvError};
pub(crate) use pusher::{Pusher, PusherT};

// Crate-wide exports
pub(crate) use endpoints::{RecvEndpoint, SendEndpoint};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    AllOperatorsInitializedOnNode(NodeId),
    OperatorInitialized(OperatorId),
    RunOperator(OperatorId),
    DataSenderInitialized(NodeId),
    DataReceiverInitialized(NodeId),
    ControlSenderInitialized(NodeId),
    ControlReceiverInitialized(NodeId),
}

impl ControlMessage {
    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub fn into_rbuf(&self) -> Result<zenoh::net::RBuf, CodecError> {
        let serialized_msg = bincode::serialize(&self).map_err(|e| CodecError::from(e))?;
        Ok(serialized_msg.into())
    }

    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub fn from_rbuf(buf: &zenoh::net::RBuf) -> Result<ControlMessage, CodecError> {
        let msg_bytes = buf.to_vec();
        let msg = bincode::deserialize(&msg_bytes).map_err(|e| CodecError::from(e))?;
        Ok(msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMetadata {
    pub stream_id: StreamId,
}

#[derive(Clone)]
pub enum InterProcessMessage {
    Serialized {
        metadata: MessageMetadata,
        bytes: BytesMut,
    },
    Deserialized {
        metadata: MessageMetadata,
        data: Arc<dyn Serializable + Send + Sync>,
    },
}

impl InterProcessMessage {
    pub fn new_serialized(bytes: BytesMut, metadata: MessageMetadata) -> Self {
        Self::Serialized { metadata, bytes }
    }

    pub fn new_deserialized(
        data: Arc<dyn Serializable + Send + Sync>,
        stream_id: StreamId,
    ) -> Self {
        Self::Deserialized {
            metadata: MessageMetadata { stream_id },
            data,
        }
    }

    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub fn into_rbuf(&self) -> Result<zenoh::net::RBuf, CodecError> {
        const HEADER_SIZE: usize = 8;
        match self {
            InterProcessMessage::Deserialized { metadata, data } => {
                let mut buf = Vec::new();
                let metadata_size =
                    bincode::serialized_size(&metadata).map_err(CodecError::from)?;
                let data_size = data.serialized_size().map_err(|e| {
                    CodecError::BincodeError(Box::new(bincode::ErrorKind::Custom(format!(
                        "{:?}",
                        e
                    ))))
                })?;
                buf.reserve(HEADER_SIZE + metadata_size as usize + data_size);
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, metadata_size as u32)?;
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, data_size as u32)?;
                bincode::serialize_into(&mut buf, &metadata).map_err(CodecError::from)?;
                let enc_data = data.encode_into_vec().map_err(CodecError::from)?;
                buf.extend_from_slice(&enc_data.as_slice());
                Ok(buf.into())
            }
            InterProcessMessage::Serialized { metadata, bytes } => {
                let mut buf = Vec::new();
                let metadata_size =
                    bincode::serialized_size(&metadata).map_err(CodecError::from)?;
                let data_size = bytes.len();
                buf.reserve(HEADER_SIZE + metadata_size as usize + data_size);
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, metadata_size as u32)?;
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, data_size as u32)?;
                bincode::serialize_into(&mut buf, &metadata).map_err(CodecError::from)?;
                let enc_data = bytes.to_vec();
                buf.extend_from_slice(&enc_data.as_slice());
                Ok(buf.into())
            }
        }
    }

    #[cfg(feature = "zenoh_zerocopy_transport")]
    pub fn into_sbuf(
        &self,
        shm: &mut zenoh::net::SharedMemoryManager,
    ) -> Result<zenoh::net::SharedMemoryBuf, CodecError> {
        const HEADER_SIZE: usize = 8;
        match self {
            InterProcessMessage::Deserialized { metadata, data } => {
                let mut buf = Vec::new();

                let metadata_size =
                    bincode::serialized_size(&metadata).map_err(CodecError::from)?;
                let data_size = data.serialized_size().map_err(|e| {
                    CodecError::BincodeError(Box::new(bincode::ErrorKind::Custom(format!(
                        "{:?}",
                        e
                    ))))
                })?;

                let tot_len = HEADER_SIZE + metadata_size as usize + data_size;
                buf.reserve(tot_len);

                let mut sbuf = shm
                    .alloc(tot_len)
                    .ok_or(CodecError::ZenohSharedMemoryError(
                        "Unable to allocate Buffer".to_string(),
                    ))?;

                let slice = unsafe { sbuf.as_mut_slice() };

                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, metadata_size as u32)?;
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, data_size as u32)?;
                bincode::serialize_into(&mut buf, &metadata).map_err(CodecError::from)?;
                let enc_data = data.encode_into_vec().map_err(CodecError::from)?;
                buf.extend_from_slice(&enc_data.as_slice());

                slice[0..tot_len].copy_from_slice(&buf.as_slice());
                Ok(sbuf)
            }
            InterProcessMessage::Serialized { metadata, bytes } => {
                let mut buf = Vec::new();
                let metadata_size =
                    bincode::serialized_size(&metadata).map_err(CodecError::from)?;
                let data_size = bytes.len();

                let tot_len = HEADER_SIZE + metadata_size as usize + data_size;

                buf.reserve(tot_len);

                let mut sbuf = shm
                    .alloc(tot_len)
                    .ok_or(CodecError::ZenohSharedMemoryError(
                        "Unable to allocate Buffer".to_string(),
                    ))?;

                let slice = unsafe { sbuf.as_mut_slice() };

                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, metadata_size as u32)?;
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buf, data_size as u32)?;
                bincode::serialize_into(&mut buf, &metadata).map_err(CodecError::from)?;
                let enc_data = bytes.to_vec();
                buf.extend_from_slice(&enc_data.as_slice());

                slice[0..tot_len].copy_from_slice(&buf.as_slice());
                Ok(sbuf)
            }
        }
    }

    #[cfg(any(feature = "zenoh_transport", feature = "zenoh_zerocopy_transport"))]
    pub fn from_rbuf(buf: &zenoh::net::RBuf) -> Result<Self, CodecError> {
        const HEADER_SIZE: usize = 8;

        let mut buf: BytesMut = BytesMut::from(&buf.to_vec()[..]);

        if buf.len() >= HEADER_SIZE {
            let header = buf.split_to(HEADER_SIZE);
            let metadata_size = NetworkEndian::read_u32(&header[0..4]) as usize;
            let data_size = NetworkEndian::read_u32(&header[4..8]) as usize;

            if buf.len() < metadata_size {
                return Err(CodecError::BincodeError(Box::new(
                    bincode::ErrorKind::Custom("Malformed message".to_string()),
                )));
            }
            let metadata_bytes = buf.split_to(metadata_size);
            let metadata: MessageMetadata =
                bincode::deserialize(&metadata_bytes).map_err(CodecError::BincodeError)?;

            if buf.len() < data_size {
                return Err(CodecError::BincodeError(Box::new(
                    bincode::ErrorKind::Custom("Malformed message".to_string()),
                )));
            }

            let bytes = buf.split_to(data_size);
            let msg = InterProcessMessage::new_serialized(bytes, metadata);
            Ok(msg)
        } else {
            Err(CodecError::BincodeError(Box::new(
                bincode::ErrorKind::Custom("Malformed message".to_string()),
            )))
        }
    }

    #[cfg(feature = "zenoh_zerocopy_transport")]
    pub fn from_sbuf(
        buf: zenoh::net::RBuf,
        shm: &mut zenoh::net::SharedMemoryManager,
    ) -> Result<Self, CodecError> {
        const HEADER_SIZE: usize = 8;

        let sbuf = buf.into_shm(shm).ok_or(CodecError::ZenohSharedMemoryError(
            "Unable to allocate Buffer".to_string(),
        ))?;

        let mut buf: BytesMut = BytesMut::from(sbuf.as_slice());

        // Shared memory is not more needed
        drop(sbuf);

        if buf.len() >= HEADER_SIZE {
            let header = buf.split_to(HEADER_SIZE);
            let metadata_size = NetworkEndian::read_u32(&header[0..4]) as usize;
            let data_size = NetworkEndian::read_u32(&header[4..8]) as usize;

            if buf.len() < metadata_size {
                return Err(CodecError::BincodeError(Box::new(
                    bincode::ErrorKind::Custom("Malformed message".to_string()),
                )));
            }
            let metadata_bytes = buf.split_to(metadata_size);
            let metadata: MessageMetadata =
                bincode::deserialize(&metadata_bytes).map_err(CodecError::BincodeError)?;

            if buf.len() < data_size {
                return Err(CodecError::BincodeError(Box::new(
                    bincode::ErrorKind::Custom("Malformed message".to_string()),
                )));
            }

            let bytes = buf.split_to(data_size);
            let msg = InterProcessMessage::new_serialized(bytes, metadata);
            
            Ok(msg)
        } else {
            Err(CodecError::BincodeError(Box::new(
                bincode::ErrorKind::Custom("Malformed message".to_string()),
            )))
        }
    }
}

impl std::fmt::Debug for InterProcessMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InterProcessMessage::Deserialized { metadata, data: _ } => f
                .debug_struct("InterProcessMessage")
                .field("Kind:", &"Deserialized".to_string())
                .field("Metadata", metadata)
                .finish(),
            InterProcessMessage::Serialized { metadata, bytes } => f
                .debug_struct("InterProcessMessage")
                .field("Kind", &"Serialized".to_string())
                .field("Metadata", metadata)
                .field("Bytes", bytes)
                .finish(),
        }
    }
}

/// Returns a vec of TCPStreams; one for each node pair.
///
/// The function creates a TCPStream to each node address. The node address vector stores
/// the network address of each node, and is indexed by node id.
pub async fn create_tcp_streams(
    node_addrs: Vec<SocketAddr>,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Vec<(NodeId, TcpStream)> {
    let node_addr = node_addrs[node_id].clone();
    // Connect to the nodes that have a lower id than the node.
    let connect_streams_fut = connect_to_nodes(node_addrs[..node_id].to_vec(), node_id, logger);
    // Wait for connections from the nodes that have a higher id than the node.
    let stream_fut = await_node_connections(node_addr, node_addrs.len() - node_id - 1, logger);
    // Wait until all connections are established.
    match future::try_join(connect_streams_fut, stream_fut).await {
        Ok((mut streams, await_streams)) => {
            // Streams contains a TCP stream for each other node.
            streams.extend(await_streams);
            streams
        }
        Err(e) => {
            slog::error!(
                logger,
                "Node {}: creating TCP streams errored with {:?}",
                node_id,
                e
            );
            panic!(
                "Node {}: creating TCP streams errored with {:?}",
                node_id, e
            )
        }
    }
}

/// Connects to all addresses and sends node id.
///
/// The function returns a vector of `(NodeId, TcpStream)` for each connection.
async fn connect_to_nodes(
    addrs: Vec<SocketAddr>,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Result<Vec<(NodeId, TcpStream)>, std::io::Error> {
    let mut connect_futures = Vec::new();
    // For each node address, launch a task that tries to create a TCP stream to the node.
    for addr in addrs.iter() {
        connect_futures.push(connect_to_node(addr, node_id, logger));
    }
    // Wait for all tasks to complete successfully.
    let tcp_results = future::try_join_all(connect_futures).await?;
    let streams: Vec<(NodeId, TcpStream)> = (0..tcp_results.len()).zip(tcp_results).collect();
    Ok(streams)
}

/// Creates TCP stream connection to an address and writes the node id on the TCP stream.
///
/// The function keeps on retrying until it connects successfully.
async fn connect_to_node(
    dst_addr: &SocketAddr,
    node_id: NodeId,
    logger: &slog::Logger,
) -> Result<TcpStream, std::io::Error> {
    // Keeps on reatying to connect to `dst_addr` until it succeeds.
    let mut last_err_msg_time = Instant::now();
    loop {
        match TcpStream::connect(dst_addr).await {
            Ok(mut stream) => {
                stream.set_nodelay(true).expect("couldn't disable Nagle");
                // Send the node id so that the TCP server knows with which
                // node the connection was established.
                let mut buffer: Vec<u8> = Vec::new();
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buffer, node_id as u32)?;
                loop {
                    match stream.write(&buffer[..]).await {
                        Ok(_) => return Ok(stream),
                        Err(e) => {
                            slog::error!(
                                logger,
                                "Node {}: could not send node id to {}; error {}; retrying in 100 ms",
                                node_id,
                                dst_addr,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                // Only print connection errors every 1s.
                let now = Instant::now();
                if now.duration_since(last_err_msg_time) >= Duration::from_secs(1) {
                    slog::error!(
                        logger,
                        "Node {}: could not connect to {}; error {}; retrying",
                        node_id,
                        dst_addr,
                        e
                    );
                    last_err_msg_time = now;
                }
                // Wait a bit until it tries to connect again.
                delay_for(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Awaiting for connections from `expected_conns` other nodes.
///
/// Upon a new connection, the function reads from the stream the id of the node that initiated
/// the connection.
async fn await_node_connections(
    addr: SocketAddr,
    expected_conns: usize,
    logger: &slog::Logger,
) -> Result<Vec<(NodeId, TcpStream)>, std::io::Error> {
    let mut await_futures = Vec::new();
    let mut listener = TcpListener::bind(&addr).await?;
    // Awaiting for `expected_conns` conections.
    for _ in 0..expected_conns {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true).expect("couldn't disable Nagle");
        // Launch a task that reads the node id from the TCP stream.
        await_futures.push(read_node_id(stream, logger));
    }
    // Await until we've received `expected_conns` node ids.
    Ok(future::try_join_all(await_futures).await?)
}

/// Reads a node id from a TCP stream.
///
/// The method is used to discover the id of the node that initiated the connection.
async fn read_node_id(
    mut stream: TcpStream,
    logger: &slog::Logger,
) -> Result<(NodeId, TcpStream), std::io::Error> {
    let mut buffer = [0u8; 4];
    match stream.read_exact(&mut buffer).await {
        Ok(n) => n,
        Err(e) => {
            slog::error!(logger, "failed to read from socket; err = {:?}", e);
            return Err(e);
        }
    };
    let node_id: u32 = NetworkEndian::read_u32(&buffer);
    Ok((node_id as NodeId, stream))
}
