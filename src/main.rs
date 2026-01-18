use std::{collections::HashMap, result, sync::Arc};

use axum::{Json, extract::State, response};
use clap::Parser;
use lettre::{Address, AsyncTransport, message};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DbErr, EntityTrait, ModelTrait,
    QueryFilter,
};
use serde::Deserialize;
use tokio::{fs, sync::Mutex, time};

mod database;
use crate::database::{prelude::*, station, user, user_station};

#[tokio::main]
async fn main() {
    use axum::routing;
    use std::net;

    let args = Args::parse();

    #[cfg(feature = "log")]
    env_logger::Builder::new()
        .filter_level(args.verbosity.log_level_filter())
        .init();

    let mailbox = message::Mailbox::new(args.username.clone(), args.address.clone());
    let username = args.username.unwrap_or_else(|| args.address.to_string());
    let transport = lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(&args.relay)
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
        time::Duration::from_secs(args.timeout.into()),
    )));

    let router = axum::Router::new()
        .route("/", routing::get(index))
        .route("/index.html", routing::get(index))
        .route("/index.js", routing::get(script))
        .route("/get_lines", routing::get(get_lines))
        .with_state(state.clone())
        .route("/get_stations", routing::get(get_stations))
        .with_state(state.clone())
        .route("/submit_email", routing::post(submit_email))
        .with_state(state.clone())
        .route("/update_subscription", routing::delete(unsubscribe))
        .with_state(state.clone())
        .route("/update_subscription", routing::put(update_subscription))
        .with_state(state);

    let addr = net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1)), 3000);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind to socket");
    axum::serve(listener, router).await.expect("Server Crashed");
}

/// Subscribe to WMATA Fire-Alarm
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Email address to send from
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub address: Address,

    /// Username for the SMTP relay server, will default to address
    #[arg(short, long)]
    #[cfg_attr(feature = "env", arg(env))]
    pub username: Option<String>,

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

    #[cfg(feature = "log")]
    #[command(flatten)]
    verbosity: clap_verbosity_flag::Verbosity,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("IO error: {0}")]
    IoError(#[from] tokio::io::Error),

    #[error("Database Error: {0}")]
    DbError(#[from] DbErr),

    #[error("Invalid Email Address: {0}")]
    AddressError(#[from] lettre::address::AddressError),

    #[error("Failed to generate email message: {0}")]
    EmailError(#[from] lettre::error::Error),

    #[error("Failed to send email: {0}")]
    SendError(String),

    #[error("Email could not be found, could have timed out: {0}")]
    EmailNotFound(Address),

    #[error("Code does not match: {0}")]
    CodeDoesNotMatch(CodeType),
}

/// Allows for [Result] to work with Axum
impl response::IntoResponse for Error {
    fn into_response(self) -> response::Response {
        use axum::http::StatusCode;

        let status_code = match self {
            Error::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::AddressError(_) => StatusCode::BAD_REQUEST,
            Error::EmailError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::EmailNotFound(_) => StatusCode::REQUEST_TIMEOUT,
            Error::CodeDoesNotMatch(_) => StatusCode::UNAUTHORIZED,
        };
        let message = format!("{self:?}");

        #[cfg(feature = "log")]
        log::error!("{message}");
        #[cfg(not(feature = "log"))]
        eprintln!("{message}");

        (status_code, message).into_response()
    }
}

type Result<T> = result::Result<T, Error>;

async fn index() -> Result<response::Html<String>> {
    #[cfg(feature = "log")]
    log::debug!("Index page loaded");

    Ok(response::Html(fs::read_to_string("index.html").await?))
}

async fn script() -> Result<String> {
    #[cfg(feature = "log")]
    log::debug!("Script file loaded");

    Ok(fs::read_to_string("index.js").await?)
}

/// Randomly generated to verify the user has access to the email
type CodeType = u16;

/// Timer that determines if a verification code is valid
struct TempData {
    handle: tokio::task::AbortHandle,
    code: CodeType,
}

impl TempData {
    /// Creates a background timer than when expires will remove the verification code from the temporary database, thus invalidating it
    fn new(
        email: Address,
        state: Arc<
            Mutex<
                AppState<
                    impl AsyncTransport + Send + 'static,
                    impl ConnectionTrait + Send + 'static,
                >,
            >,
        >,
        code: CodeType,
        duration: time::Duration,
    ) -> Self {
        use tokio::time;
        Self {
            handle: tokio::spawn(async move {
                time::sleep(duration).await;
                state.lock().await.temp_db.remove(&email);
            })
            .abort_handle(),
            code: code,
        }
    }

    /// Prematurely end the background timer and checks if the verification codes match
    fn end(&self, code: CodeType) -> bool {
        self.handle.abort();
        self.code == code
    }
}

/// Used to store the [TempData] for each email address that has not been validated
type TempDb = HashMap<Address, TempData>;

/// Stores any global state needed by the application
struct AppState<T: AsyncTransport, C: ConnectionTrait> {
    temp_db: TempDb,
    message_builder: message::MessageBuilder,
    transport: T,
    db: C,
    duration: time::Duration,
}

impl<T: AsyncTransport, C: ConnectionTrait> AppState<T, C> {
    fn new(mailbox: message::Mailbox, transport: T, db: C, duration: time::Duration) -> Self {
        Self {
            temp_db: HashMap::new(),
            message_builder: lettre::Message::builder()
                .from(mailbox)
                .subject("WMATA Fire-Alarm Verification Code")
                .header(message::header::ContentType::TEXT_PLAIN),
            transport: transport,
            db: db,
            duration,
        }
    }
}

/// Fetches the list of rail lines on the network
async fn get_lines<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
) -> Result<Json<Vec<String>>> {
    #[cfg(feature = "log")]
    log::debug!("Get rail lines");

    Ok(Json(
        RailLine::find()
            .all(&state.lock().await.db)
            .await?
            .into_iter()
            .map(|line| line.name)
            .collect(),
    ))
}

/// Station name and the rail lines that it is on, for client-side filtering
#[derive(Clone, serde::Serialize)]
struct StationInfo {
    name: String,
    lines: Vec<String>,
}

/// Gets the stations and formats them as [StationInfo]
async fn get_stations<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
) -> Result<Json<Vec<StationInfo>>> {
    #[cfg(feature = "log")]
    log::debug!("Get stations");

    let db = &state.lock().await.db;
    let stations = Station::find().all(db).await?;
    let mut station_infos = Vec::with_capacity(stations.len());
    for station in stations {
        let lines = station
            .find_related(RailLine)
            .all(db)
            .await?
            .into_iter()
            .map(|line| line.name)
            .collect();
        let station_info = StationInfo {
            name: station.name,
            lines: lines,
        };
        station_infos.push(station_info);
    }
    Ok(Json(station_infos))
}

/// Starting point for modifying a subscription, will send a verification code to the given email
async fn submit_email<T, C: ConnectionTrait + Send + 'static>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
    email: String,
) -> Result<()>
where
    T: AsyncTransport + Send + Sync + 'static,
    T::Error: std::fmt::Debug,
{
    let future = state.lock();
    let address: Address = email.parse()?;
    let code: CodeType = rand::random();

    #[cfg(feature = "log")]
    log::info!("{email}: {code}");
    #[cfg(not(feature = "log"))]
    println!("{email}: {code:0>6}");

    let mut app_state = future.await;
    let duration = app_state.duration;
    let message = app_state
        .message_builder
        .clone()
        .to(address.clone().into())
        .body(format!("{code:0>6}"))?; // Pad with zero to 6 digits
    if let Some(old_entry) = app_state.temp_db.insert(
        address.clone(),
        TempData::new(address, state.clone(), code, duration),
    ) {
        old_entry.handle.abort();
    }

    match app_state.transport.send(message).await {
        Ok(_) => Ok(()),
        Err(error) => Err(Error::SendError(format!("{error:?}"))),
    }
}

/// Email address of the user and a code to verify access
#[derive(Deserialize)]
struct UserAuth {
    email: Address,
    code: CodeType,
}

impl UserAuth {
    /// Checks the code against the temporary database
    async fn auth(&self, temp_db: &mut TempDb) -> Result<()> {
        if match temp_db.remove(&self.email) {
            Some(temp_data) => temp_data.end(self.code),
            None => return Err(Error::EmailNotFound(self.email.clone())),
        } {
            Ok(())
        } else {
            Err(Error::CodeDoesNotMatch(self.code))
        }
    }
}

/// Fetches the user model from the database from the given email address
async fn get_user_by_email(
    db: &impl ConnectionTrait,
    email: &String,
) -> Result<Option<user::Model>> {
    Ok(User::find()
        .filter(user::Column::Email.eq(email))
        .one(db)
        .await?)
}

/// Completely removes the user from the service
async fn delete_user(user: user::Model, db: &impl ConnectionTrait) -> Result<()> {
    UserStation::delete_many()
        .filter(user_station::Column::UserId.eq(user.id))
        .exec(db)
        .await?;
    user.delete(db).await?;
    Ok(())
}

/// Removes a user form the service, first authenticates than checks if they are subscribed
async fn unsubscribe(
    State(state): State<Arc<Mutex<AppState<impl AsyncTransport, impl ConnectionTrait>>>>,
    Json(user_auth): Json<UserAuth>,
) -> Result<()> {
    #[cfg(feature = "log")]
    log::info!("Delete request from {}", user_auth.email);

    let mut app_state = state.lock().await;
    user_auth.auth(&mut app_state.temp_db).await?;
    #[cfg(feature = "log")]
    log::debug!("authenticated");

    Ok(
        match get_user_by_email(&app_state.db, &user_auth.email.to_string()).await? {
            Some(user) => {
                #[cfg(feature = "log")]
                log::debug!("found");
                delete_user(user, &app_state.db).await?
            }
            None => {
                #[cfg(feature = "log")]
                log::debug!("not found");
                ()
            }
        },
    )
}

/// The User and the list of stations that they want to be subscribed to
#[derive(Deserialize)]
struct Subscription {
    user_auth: UserAuth,
    stations: Vec<String>,
}

// The reason for why there are so many subfunctions for [`update_user_stations`] was to chase down a bug with unit testing. After that I figured I would leave it.

/// Fetches the station models from the list of names given
async fn get_stations_from_names(
    db: &impl ConnectionTrait,
    names: &[String],
) -> Result<Vec<station::Model>> {
    Ok(Station::find()
        .all(db)
        .await?
        .into_iter()
        .filter(|station| names.contains(&station.name))
        .collect())
}

/// Gets the stations that the user is already subscribed to
async fn get_already_selected_stations(
    user: &user::Model,
    db: &impl ConnectionTrait,
) -> Result<Vec<station::Model>> {
    Ok(user.find_related(Station).all(db).await?)
}

/// Deletes from the database any links to stations not in the list
async fn delete_user_stations_not_in_list(
    user: &user::Model,
    db: &impl ConnectionTrait,
    stations: &[station::Model],
) -> Result<()> {
    let mut user_stations = user.find_related(UserStation).all(db).await?;
    user_stations.retain(|user_station| {
        !stations
            .iter()
            .any(|station| user_station.station_id == station.id)
    });

    // Unfortunately I cannot choice specific records to delete like I can with INSERT, so I am doing them one at a time.
    for user_station in user_stations {
        user_station.delete(db).await?;
    }
    Ok(())
}

/// Removes from [`stations`] any that are in [`already_selected_stations`]
fn filter_out_stations(
    stations: &mut Vec<station::Model>,
    already_selected_stations: &[station::Model],
) {
    stations.retain(|station| !already_selected_stations.contains(station));
}

/// Removes stations from the list that the user is already subscribed to
async fn remove_already_selected_stations(
    user: &user::Model,
    db: &impl ConnectionTrait,
    stations: &mut Vec<station::Model>,
) -> Result<()> {
    let already_selected_stations = get_already_selected_stations(user, db).await?;
    filter_out_stations(stations, &already_selected_stations);
    Ok(())
}

/// Deletes links from the database that are not in the list, than removes stations that already have links from the list. Order is important
async fn delete_not_selected_user_stations(
    user: &user::Model,
    db: &impl ConnectionTrait,
    stations: &mut Vec<station::Model>,
) -> Result<()> {
    delete_user_stations_not_in_list(&user, db, stations).await?;
    remove_already_selected_stations(&user, db, stations).await
}

/// Business logic for updating the subscriptions of a user
async fn update_user_stations(
    db: &impl ConnectionTrait,
    names: &[String],
    email: String,
) -> Result<()> {
    let mut stations = get_stations_from_names(db, names).await?;
    #[cfg(feature = "log")]
    log::trace!("Got list of stations from selected station names");

    let user = match get_user_by_email(db, &email).await? {
        Some(user) => {
            #[cfg(feature = "log")]
            log::debug!("{email} already in database");
            delete_not_selected_user_stations(&user, db, &mut stations).await?;
            #[cfg(feature = "log")]
            log::debug!("Deleted stations previous linked to {email} but no longer selected");
            user
        }
        None => {
            #[cfg(feature = "log")]
            log::debug!("{email} not found in database, creating new user");
            user::ActiveModel {
                email: ActiveValue::Set(email),
                ..Default::default()
            }
            .insert(db)
            .await?
        }
    };

    UserStation::insert_many(
        stations
            .into_iter()
            .map(|station| user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }),
    )
    .on_empty_do_nothing()
    .exec_without_returning(db)
    .await?;
    #[cfg(feature = "log")]
    log::debug!("Inserted new links between user and selected stations");
    Ok(())
}

/// Wrapper for [update_user_stations] that includes authentication
async fn update_subscription<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
    Json(subscription): Json<Subscription>,
) -> Result<()> {
    #[cfg(feature = "log")]
    log::debug!(
        "Update subscriptions request from {}",
        subscription.user_auth.email
    );
    let mut app_state = state.lock().await;
    subscription.user_auth.auth(&mut app_state.temp_db).await?; // Will error out here if auth fails
    #[cfg(feature = "log")]
    log::trace!("authenticated");

    update_user_stations(
        &app_state.db,
        &subscription.stations,
        subscription.user_auth.email.to_string(),
    )
    .await
}

#[cfg(test)]
mod test {
    use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait, ModelTrait};

    use super::{
        Error,
        database::{prelude::*, station, user, user_station},
        update_user_stations,
    };

    /// Creates a in-memory SQLite database with dummy stations (taken from the actual WMATA network) for testing
    async fn setup_test_db() -> Result<sea_orm::DatabaseConnection, sea_orm::DbErr> {
        use sea_orm::ConnectionTrait;

        let db = sea_orm::Database::connect("sqlite::memory:").await?;
        let backend = db.get_database_backend();
        let schema = sea_orm::Schema::new(backend);

        let table_create_statements = [
            schema.create_table_from_entity(RailLine),
            schema.create_table_from_entity(User),
            schema.create_table_from_entity(Station),
            schema.create_table_from_entity(UserStation),
        ];
        for statement in table_create_statements {
            db.execute(backend.build(&statement)).await?;
        }

        let stations = [
            station::ActiveModel {
                id: ActiveValue::Set(44),
                name: ActiveValue::Set(String::from("Herndon")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(69),
                name: ActiveValue::Set(String::from("Reston Town Center")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(97),
                name: ActiveValue::Set(String::from("Wiehle-Reston East")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(79),
                name: ActiveValue::Set(String::from("Spring Hill")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(42),
                name: ActiveValue::Set(String::from("Greensboro")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(85),
                name: ActiveValue::Set(String::from("Tysons")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(53),
                name: ActiveValue::Set(String::from("McLean")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(26),
                name: ActiveValue::Set(String::from("East Falls Church")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(6),
                name: ActiveValue::Set(String::from("Ballston-MU")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(91),
                name: ActiveValue::Set(String::from("Virginia Square-GMU")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(15),
                name: ActiveValue::Set(String::from("Clarendon")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(20),
                name: ActiveValue::Set(String::from("Court House")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(73),
                name: ActiveValue::Set(String::from("Rosslyn")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(33),
                name: ActiveValue::Set(String::from("Foggy Bottom-GWU")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(30),
                name: ActiveValue::Set(String::from("Farragut West")),
            },
            station::ActiveModel {
                id: ActiveValue::Set(54),
                name: ActiveValue::Set(String::from("McPherson Square")),
            },
        ];

        Station::insert_many(stations)
            .on_conflict_do_nothing()
            .exec_without_returning(&db)
            .await?;
        Ok(db)
    }

    #[tokio::test]
    async fn get_user_by_email_test() {
        use super::get_user_by_email;

        let db = setup_test_db().await.unwrap();
        let email = String::from("general.konobi@jedi.com");
        assert!(get_user_by_email(&db, &email).await.unwrap().is_none());

        let user = user::ActiveModel {
            email: ActiveValue::Set(email.clone()),
            ..Default::default()
        };
        let expected = user.insert(&db).await.unwrap();

        let received_user = get_user_by_email(&db, &email).await.unwrap().unwrap();
        assert_eq!(expected, received_user);
    }

    #[tokio::test]
    async fn delete_user_test() {
        let db = setup_test_db().await.unwrap();
        let user = user::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();
        UserStation::insert_many([6, 15, 26, 42, 44, 53, 69, 79, 85, 91, 97].map(|id| {
            user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(id),
            }
        }))
        .on_conflict_do_nothing()
        .exec_without_returning(&db)
        .await
        .unwrap();

        super::delete_user(user, &db).await.unwrap();
    }

    #[tokio::test]
    async fn get_stations_from_names_test() {
        let db = setup_test_db().await.unwrap();
        let names = [
            "Herndon",
            "Reston Town Center",
            "Wiehle-Reston East",
            "Spring Hill",
            "Greensboro",
            "Tysons",
            "McLean",
            "East Falls Church",
            "Ballston-MU",
            "Virginia Square-GMU",
            "Clarendon",
        ]
        .map(|name| name.to_string());
        let stations = super::get_stations_from_names(&db, &names).await.unwrap();
        let expected = [
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];
        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn get_already_selected_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = user::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = [
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        let user_stations: Vec<_> = stations
            .iter()
            .map(|station| user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            })
            .collect();
        UserStation::insert_many(user_stations)
            .on_empty_do_nothing()
            .exec_without_returning(&db)
            .await
            .unwrap();

        let already_selected_stations = super::get_already_selected_stations(&user, &db)
            .await
            .unwrap();
        assert_eq!(already_selected_stations, stations);
    }

    #[test]
    fn filter_out_stations_test() {
        let already_selected_stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
        ];

        let mut stations = vec![
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        super::filter_out_stations(&mut stations, &already_selected_stations);

        let expected = [
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn remove_already_selected_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = user::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let already_selected_stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
        ];

        UserStation::insert_many(already_selected_stations.map(|station| {
            user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }
        }))
        .on_empty_do_nothing()
        .exec_without_returning(&db)
        .await
        .unwrap();

        let mut stations = vec![
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        super::remove_already_selected_stations(&user, &db, &mut stations)
            .await
            .unwrap();

        let expected = [
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn delete_user_stations_not_in_list_test() {
        let db = setup_test_db().await.unwrap();
        let user = user::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = vec![
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        let user_stations: Vec<_> = stations
            .iter()
            .map(|station| user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            })
            .collect();
        UserStation::insert_many(user_stations)
            .on_conflict_do_nothing()
            .exec_without_returning(&db)
            .await
            .unwrap();

        let expected = [
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        super::delete_user_stations_not_in_list(&user, &db, &expected)
            .await
            .unwrap();

        let result = user.find_related(Station).all(&db).await.unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn delete_not_selected_user_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = user::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        UserStation::insert_many(stations.map(|station| user_station::ActiveModel {
            user_id: ActiveValue::Set(user.id),
            station_id: ActiveValue::Set(station.id),
        }))
        .on_conflict_do_nothing()
        .exec_without_returning(&db)
        .await
        .unwrap();

        let mut selected_stations = vec![
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        let mut expected = selected_stations.clone();
        expected.sort_by_cached_key(|station| station.id);

        super::delete_not_selected_user_stations(&user, &db, &mut selected_stations)
            .await
            .unwrap();
        assert!(selected_stations.is_empty());

        let current_stations = user.find_related(Station).all(&db).await.unwrap();
        assert_eq!(current_stations, expected);
    }

    #[tokio::test]
    async fn update_user_stations_test_new_user() {
        let db = setup_test_db().await.unwrap();
        let stations = [
            "Herndon",
            "Reston Town Center",
            "Wiehle-Reston East",
            "Spring Hill",
            "Greensboro",
            "Tysons",
            "McLean",
            "East Falls Church",
            "Ballston-MU",
            "Virginia Square-GMU",
            "Clarendon",
        ]
        .map(|name| name.to_string());
        update_user_stations(&db, &stations, String::from("general.konobi@jedi.com"))
            .await
            .unwrap();

        let user_stations = User::find_by_id(1)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .find_related(UserStation)
            .all(&db)
            .await
            .unwrap();
        let expected =
            [6, 15, 26, 42, 44, 53, 69, 79, 85, 91, 97].map(|stations_id| user_station::Model {
                user_id: 1,
                station_id: stations_id,
            });
        assert_eq!(user_stations, expected);
    }

    async fn update_user_stations_existing_user_test_framework(
        starting_stations: &[station::Model],
        stations: &[String],
    ) -> Result<Vec<station::Model>, Error> {
        let db = setup_test_db().await?;
        let user = user::ActiveModel::default_values().insert(&db).await?;
        UserStation::insert_many(starting_stations.into_iter().map(|station| {
            user_station::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }
        }))
        .on_conflict_do_nothing()
        .exec_without_returning(&db)
        .await?;

        update_user_stations(&db, &stations, user.email.clone()).await?;
        Ok(user.find_related(Station).all(&db).await?)
    }

    #[tokio::test]
    async fn update_user_stations_test_add_stations() {
        let starting_stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
        ];

        let stations = [
            "Herndon",
            "Reston Town Center",
            "Wiehle-Reston East",
            "Spring Hill",
            "Greensboro",
            "Tysons",
            "McLean",
            "East Falls Church",
            "Ballston-MU",
            "Virginia Square-GMU",
            "Clarendon",
        ]
        .map(|name| name.to_string());

        let expected = [
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        assert_eq!(stations.len(), expected.len());
        let result =
            update_user_stations_existing_user_test_framework(&starting_stations, &stations)
                .await
                .unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn update_user_stations_test_remove_stations() {
        let starting_stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        let stations = [
            "Herndon",
            "Reston Town Center",
            "Wiehle-Reston East",
            "Spring Hill",
            "Greensboro",
        ]
        .map(|name| name.to_string());

        let expected = [
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        assert_eq!(stations.len(), expected.len());
        let result =
            update_user_stations_existing_user_test_framework(&starting_stations, &stations)
                .await
                .unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn update_user_stations_test_add_remove_stations() {
        let starting_stations = [
            station::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            station::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            station::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            station::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            station::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        let stations = [
            "Tysons",
            "McLean",
            "East Falls Church",
            "Ballston-MU",
            "Virginia Square-GMU",
            "Clarendon",
            "Court House",
            "Rosslyn",
            "Foggy Bottom-GWU",
            "Farragut West",
            "McPherson Square",
        ]
        .map(|name| name.to_string());

        let expected = [
            station::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            station::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            station::Model {
                id: 20,
                name: String::from("Court House"),
            },
            station::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            station::Model {
                id: 30,
                name: String::from("Farragut West"),
            },
            station::Model {
                id: 33,
                name: String::from("Foggy Bottom-GWU"),
            },
            station::Model {
                id: 53,
                name: String::from("McLean"),
            },
            station::Model {
                id: 54,
                name: String::from("McPherson Square"),
            },
            station::Model {
                id: 73,
                name: String::from("Rosslyn"),
            },
            station::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            station::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
        ];

        assert_eq!(stations.len(), expected.len());
        let result =
            update_user_stations_existing_user_test_framework(&starting_stations, &stations)
                .await
                .unwrap();
        assert_eq!(result, expected);
    }
}
