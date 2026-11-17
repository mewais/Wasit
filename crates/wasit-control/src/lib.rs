//! Wasit control protocol (ARCHITECTURE.md §4): wire types, newline-delimited JSON framing,
//! a symmetric connection (client and server), a supervised reconnecting client with durable
//! subscriptions, child-process transports, the link supervisor/router (`Hub`) and the stdio
//! bridge.
//!
//! This crate is generic: concrete methods and topics are defined by higher layers through the
//! [`Service`] trait with JSON payloads.

pub mod bridge;
pub mod client;
pub mod command;
pub mod connection;
pub mod demo;
pub mod error;
pub mod hub;
pub mod mux;
pub mod server;
pub mod service;
pub mod supervised;
pub mod transport;
pub mod wire;

pub use bridge::{bridge, BridgeError, BridgeOptions};
pub use client::{ClientOptions, ControlClient, Diagnostics};
pub use command::{
    classify_command_failure, ChildHandle, CommandFailureKind, CommandSpec, CommandTransport,
    ExitInfo,
};
pub use connection::{
    ConnConfig, Connection, Negotiated, RequestOpts, SubscribeOpts, Subscription,
};
pub use error::{CallError, ConnClose, ConnectError};
pub use hub::{
    CommandConnector, HostSpec, Hub, HubConfig, HubError, LinkConnector, LinkEvent, Provisioner,
    RoutingService,
};
pub use mux::{MuxError, ServiceMux};
pub use server::ControlServer;
pub use service::{CallContext, EventStream, NoService, Service};
pub use supervised::{
    connector, Backoff, Connector, DurableEnd, DurableOpts, DurableSubscription, LinkState,
    ProvisionError, ProvisionReason, Supervised, SupervisedConfig, SupervisedOptions,
    SupervisorHooks,
};
#[cfg(windows)]
pub use transport::NamedPipeListener;
#[cfg(unix)]
pub use transport::UnixSocketListener;
pub use transport::{
    join_duplex, lock_path_for, memory_pair, BindError, BoxDuplex, Duplex, Endpoint, InstanceLock,
    Listener, LockError, StdioListener,
};
pub use wire::{
    codes, CloseReason, Cursor, Frame, Hello, HostId, ProtocolVersion, RpcError, SubId, PROTOCOL,
};
