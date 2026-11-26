//! Wasit control protocol (ARCHITECTURE.md §4): wire types, newline-delimited JSON framing,
//! a symmetric connection (client and server), a supervised reconnecting client with durable
//! subscriptions, child-process transports, the link supervisor/router (`Hub`) and the stdio
//! bridge.
//!
//! This crate is generic: concrete methods and topics are defined by higher layers through the
//! [`Service`] trait with JSON payloads.

mod attempt;
pub mod bridge;
pub mod client;
pub mod command;
pub mod connection;
pub mod demo;
mod diag;
pub mod error;
pub mod hub;
pub mod mux;
pub mod server;
pub mod service;
pub mod supervised;
pub mod transport;
pub mod uplink;
pub mod wire;

pub use attempt::Attempt;
pub use bridge::{bridge, BridgeError, BridgeOptions, STEP_STARTING_DAEMON};
pub use client::{ClientOptions, ControlClient, Diagnostics, TransportFacts};
pub use command::{
    classify_command_failure, classify_helper_exit, ChildHandle, CommandFailureKind, CommandSpec,
    CommandTransport, ExitInfo, TransportKind, TransportTrace, CLOSED_EXIT_WAIT,
};
pub use connection::{
    ConnConfig, Connection, HelloTiming, Negotiated, RequestOpts, SubscribeOpts, Subscription,
};
pub use diag::ConnectDiag;
pub use error::{peer_gone, CallError, ConnClose, ConnectError};
pub use hub::{
    CommandConnector, HostSpec, Hub, HubConfig, HubError, LinkConnector, LinkEvent, LinkGate,
    LinkPolicy, Provisioner, RoutingService,
};
pub use mux::{MuxError, ServiceMux};
pub use server::ControlServer;
pub use service::{CallContext, EventStream, NoService, Service};
pub use supervised::{
    connector, connector_reporting, connector_with_attempt, Backoff, Connector, DurableEnd,
    DurableOpts, DurableSubscription, LinkState, ProvisionError, ProvisionReason, Supervised,
    SupervisedConfig, SupervisedOptions, SupervisorHooks,
};
#[cfg(windows)]
pub use transport::NamedPipeListener;
#[cfg(unix)]
pub use transport::UnixSocketListener;
pub use transport::{
    bind_local_exclusive, join_duplex, local_endpoint, lock_path_for, memory_pair,
    starting_lock_path, wait_until_started, BindError, BoxDuplex, Duplex, Endpoint, InstanceLock,
    Listener, LockError, StartingLock, StdioListener,
};
pub use uplink::{Uplink, UplinkChanges};
pub use wire::{
    codes, CloseReason, Cursor, Frame, Hello, HostId, InvalidTraceId, ProtocolVersion, RpcError,
    SubId, TraceId, PROTOCOL,
};
