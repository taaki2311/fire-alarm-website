use std::{net, sync::Arc};

use clap::Parser;

use fire_alarm_website::AppState;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    use axum::routing;

    let args = Args::parse();

    let mailbox = lettre::message::Mailbox::new(args.name.clone(), args.address.clone());
    let username = args.name.unwrap_or_else(|| args.address.to_string());
    let transport =
        lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::starttls_relay(&args.relay)
            .expect("Failed to connect to SMTP server")
            .credentials(lettre::transport::smtp::authentication::Credentials::new(
                username,
                args.password,
            ))
            .build();

    let db = sea_orm::Database::connect(args.database)
        .await
        .expect("Failed to connect to SQL database");

    let state = Arc::new(Mutex::new(
        AppState::new(mailbox, transport, db, args.timeout.into())
            .expect("Failed to parse template"),
    ));

    let router = axum::Router::new()
        .route("/", routing::get(fire_alarm_website::index))
        .route("/index.html", routing::get(fire_alarm_website::index))
        .route("/index.js", routing::get(fire_alarm_website::script))
        .route("/style.css", routing::get(fire_alarm_website::style))
        .route(
            "/submit_email",
            routing::post(fire_alarm_website::submit_email),
        )
        .with_state(state.clone())
        .route(
            "/update_subscription",
            routing::delete(fire_alarm_website::unsubscribe)
                .put(fire_alarm_website::update_subscription),
        )
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(args.url)
        .await
        .expect("Failed to bind to socket");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(state))
        .await
        .expect("Server Crashed");
}

/// Subscribe to Fire-Alarm
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Email address to send from
    #[arg(short, long, default_value_t = lettre::Address::new("no-reply", "fire-alarm.org").unwrap())]
    #[cfg_attr(feature = "env", arg(env))]
    pub address: lettre::Address,

    /// Username for the SMTP relay server, will default to address
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub name: Option<String>,

    /// Password for the SMTP relay server
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub password: String,

    /// URL of the SMTP relay server
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub relay: String,

    /// URL for the SQL database
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub database: sea_orm::ConnectOptions,

    /// Timeout for authenticating the user's email
    #[arg(short, long, default_value_t = tokio::time::Duration::from_mins(5).into())]
    #[cfg_attr(feature = "env", arg(env))]
    pub timeout: humantime::Duration,

    /// URL for Server
    #[arg(short, long, default_value_t = net::SocketAddr::V4(net::SocketAddrV4::new(net::Ipv4Addr::new(127, 0, 0, 1), 8080)))]
    #[cfg_attr(feature = "env", arg(env))]
    pub url: net::SocketAddr,
}

/// Waits on receiving a signal from Standard-In to start shutting down the server.
async fn shutdown_signal<T: lettre::AsyncTransport, C: sea_orm::ConnectionTrait>(
    state: Arc<Mutex<AppState<T, C>>>,
) {
    println!("Send Interrupt Signal (SIGINT/Ctrl-C) to shutdown server");
    tokio::signal::ctrl_c()
        .await
        .inspect_err(|err| eprintln!("Interrupt Signal Error: {err}"))
        .unwrap_or_default();
    if let Some(shutdown_signal) = { state.lock().await.shutdown() } {
        // Need to put 'state.lock()' into its own block so that the lock get released
        println!("Waiting on some One-Time Passcodes that are still valid");
        shutdown_signal
            .await
            .inspect_err(|err| eprintln!("Shutdown Signal Error: {err}"))
            .unwrap_or_default()
    }
}
