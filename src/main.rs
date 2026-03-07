use std::sync::Arc;

use axum::response::Html;
use axum_extra::response::{Css, JavaScript};
use clap::Parser;

use fire_alarm_website::{AppState, Result};
use tokio::{fs, sync::Mutex};

#[tokio::main]
async fn main() {
    use axum::routing;
    use std::net;

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

    let state = Arc::new(Mutex::new(AppState::new(
        mailbox,
        transport,
        db,
        tokio::time::Duration::from_secs(args.timeout.into()),
    )));

    let router = axum::Router::new()
        .route("/", routing::get(index))
        .route("/index.html", routing::get(index))
        .route("/index.js", routing::get(script))
        .route("/style.css", routing::get(style))
        .route("/get_lines", routing::get(fire_alarm_website::get_lines))
        .with_state(state.clone())
        .route(
            "/get_stations",
            routing::get(fire_alarm_website::get_stations),
        )
        .with_state(state.clone())
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

    let addr = net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1)), 3000);
    let listener = tokio::net::TcpListener::bind(addr)
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
    #[arg(short, long)]
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

    /// Timeout for authenticating the user's email in seconds (0-65535)
    #[arg(short, long, default_value_t = 600)]
    #[cfg_attr(feature = "env", arg(env))]
    pub timeout: u16,
}

/// Serve `index.html` from the file system
async fn index() -> Result<Html<String>> {
    Ok(Html(fs::read_to_string("index.html").await?))
}

/// Serve `index.js` from the file system
async fn script() -> Result<JavaScript<String>> {
    Ok(JavaScript(fs::read_to_string("index.js").await?))
}

/// Serve `style.css` from the file system
async fn style() -> Result<Css<String>> {
    Ok(Css(fs::read_to_string("style.css").await?))
}

/// Waits on receiving a signal from Standard-In to start shutting down the server.
async fn shutdown_signal<T: lettre::AsyncTransport, C: sea_orm::ConnectionTrait>(
    state: Arc<Mutex<AppState<T, C>>>,
) {
    use tokio::io::AsyncReadExt;
    println!("Press 'Enter' to shutdown server");
    tokio::io::stdin()
        .read_u8()
        .await
        .inspect_err(|err| eprintln!("Failed to read from Standard-In: {err}"))
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
