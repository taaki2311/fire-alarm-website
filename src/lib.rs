use std::{collections::HashMap, result, sync::Arc};

use axum::{
    Json,
    extract::State,
    response::{IntoResponse, Response},
};
use lettre::{Address, AsyncTransport, message};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DbErr, EntityTrait, ModelTrait,
    QueryFilter,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, oneshot},
    time,
};

mod database;
use crate::database::{prelude::*, rail_lines, stations, user_stations, users};

/// All possible errors that the website could encounter
#[derive(Debug, thiserror::Error)]
pub enum Error {
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

    #[error("Server is shutting down and cannot accept new submissions")]
    ServerShutdown,
}

impl IntoResponse for Error {
    /// Allows for [`Result`] to work with Axum
    fn into_response(self) -> Response {
        use axum::http::StatusCode;

        let status_code = match self {
            Error::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::AddressError(_) => StatusCode::BAD_REQUEST,
            Error::EmailError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::EmailNotFound(_) => StatusCode::GONE,
            Error::CodeDoesNotMatch(_) => StatusCode::UNAUTHORIZED,
            Error::ServerShutdown => StatusCode::SERVICE_UNAVAILABLE,
        };
        let message = format!("{self:?}");
        eprintln!("{message}");
        (status_code, message).into_response()
    }
}

pub type Result<T> = result::Result<T, Error>;

/// Randomly generated to verify the user has access to the email
type CodeType = u16;

/// A one-time passcode needs to store both the code a handle to a timer during which the code is valid
struct OneTimePasscode {
    handle: tokio::task::AbortHandle,
    code: CodeType,
}

impl OneTimePasscode {
    /// Creates a background timer than when expires will remove the verification code from the OTP database, thus invalidating it
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
                state
                    .lock()
                    .await
                    .otp_db
                    .remove(&email)
                    .inspect_err(|err| eprintln!("Failed to remove timed out OTP: {err}"))
                    .unwrap_or_default();
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

/// Database for managing the one-time passcodes
struct OtpDb {
    otp_map: HashMap<Address, OneTimePasscode>,
    duration: time::Duration,
    shutdown_signal: Option<oneshot::Sender<()>>,
}

impl OtpDb {
    /// The duration gets stored here to set the timer for every new OTP
    fn new(duration: time::Duration) -> Self {
        return OtpDb {
            otp_map: HashMap::new(),
            duration: duration,
            shutdown_signal: None,
        };
    }

    /// Will only insert if a shutdown request has not been received, will return the old code if it exists
    fn insert<T: AsyncTransport + Send + 'static, C: ConnectionTrait + Send + 'static>(
        &mut self,
        address: Address,
        state: Arc<Mutex<AppState<T, C>>>,
        code: u16,
    ) -> Result<Option<OneTimePasscode>> {
        if self.shutdown_signal.is_none() {
            Ok(self.otp_map.insert(
                address.clone(),
                OneTimePasscode::new(address, state, code, self.duration),
            ))
        } else {
            Err(Error::ServerShutdown)
        }
    }

    /// If the shutdown request was received and the given address was the last one in the database, it will send the shutdown command
    fn remove(&mut self, address: &Address) -> Result<Option<OneTimePasscode>> {
        let old_entry = self.otp_map.remove(address);
        if self.otp_map.is_empty()
            && let Some(tx) = self.shutdown_signal.take()
        {
            tx.send(()).map_err(|_| Error::ServerShutdown)?;
        }
        Ok(old_entry)
    }

    /// Requests a shutdown, wait on the returned oneshot receiver if needed for the shutdown signal
    fn shutdown(&mut self) -> Option<oneshot::Receiver<()>> {
        if self.otp_map.is_empty() {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            self.shutdown_signal = Some(tx);
            Some(rx)
        }
    }
}

/// Stores any global state needed by the application
pub struct AppState<T: AsyncTransport, C: ConnectionTrait> {
    otp_db: OtpDb,
    message_builder: message::MessageBuilder,
    transport: T,
    db: C,
}

impl<T: AsyncTransport, C: ConnectionTrait> AppState<T, C> {
    /// Pre-build the message as much as it can and passes the timeout duration to the internal [`OtpDb`]
    pub fn new(mailbox: message::Mailbox, transport: T, db: C, duration: time::Duration) -> Self {
        Self {
            otp_db: OtpDb::new(duration),
            message_builder: lettre::Message::builder()
                .from(mailbox)
                .subject("Fire-Alarm Verification Code")
                .header(message::header::ContentType::TEXT_PLAIN),
            transport: transport,
            db: db,
        }
    }

    pub fn shutdown(&mut self) -> Option<oneshot::Receiver<()>> {
        self.otp_db.shutdown()
    }
}

#[derive(Serialize)]
pub struct LineInfo {
    name: String,
    red: u8,
    green: u8,
    blue: u8,
}

fn clamp_conversion(value: i16) -> u8 {
    value.clamp(u8::MIN.into(), u8::MAX.into()) as u8
}

impl From<rail_lines::Model> for LineInfo {
    fn from(value: rail_lines::Model) -> Self {
        LineInfo {
            name: value.name,
            red: clamp_conversion(value.red),
            green: clamp_conversion(value.green),
            blue: clamp_conversion(value.blue),
        }
    }
}

/// Fetches the list of rail lines on the network
pub async fn get_lines<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
) -> Result<Json<Vec<LineInfo>>> {
    Ok(Json(
        RailLines::find()
            .all(&state.lock().await.db)
            .await?
            .into_iter()
            .map(|line| line.into())
            .collect(),
    ))
}

/// Station name and the rail lines that it is on, for client-side filtering
#[derive(Clone, Serialize)]
pub struct StationInfo {
    name: String,
    lines: Vec<String>,
}

/// Gets the stations and formats them as [StationInfo]
pub async fn get_stations<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
) -> Result<Json<Vec<StationInfo>>> {
    let db = &state.lock().await.db;
    let stations = Stations::find().all(db).await?;
    let mut station_infos = Vec::with_capacity(stations.len());
    for station in stations {
        let lines = station
            .find_related(RailLines)
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
pub async fn submit_email<T, C: ConnectionTrait + Send + 'static>(
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
    let mut app_state = future.await;
    if let Some(old_entry) = app_state
        .otp_db
        .insert(address.clone(), state.clone(), code)?
    {
        old_entry.handle.abort();
    }
    println!("{email}: {code:0>6}");

    let message = app_state
        .message_builder
        .clone()
        .to(address.into())
        .body(format!("{code:0>6}"))?; // Pad with zero to 6 digits

    match app_state.transport.send(message).await {
        Ok(_) => Ok(()),
        Err(error) => Err(Error::SendError(format!("{error:?}"))),
    }
}

/// Email address of the user and a code to verify access
#[derive(Deserialize)]
pub struct UserAuth {
    email: Address,
    code: CodeType,
}

impl UserAuth {
    /// Checks the code against the OTP database
    async fn auth(&self, otp_db: &mut OtpDb) -> Result<()> {
        if match otp_db.remove(&self.email)? {
            Some(otp_data) => otp_data.end(self.code),
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
) -> Result<Option<users::Model>> {
    Ok(Users::find()
        .filter(users::Column::Email.eq(email))
        .one(db)
        .await?)
}

/// Completely removes the user from the service
async fn delete_user(user: users::Model, db: &impl ConnectionTrait) -> Result<()> {
    UserStations::delete_many()
        .filter(user_stations::Column::UserId.eq(user.id))
        .exec(db)
        .await?;
    user.delete(db).await?;
    Ok(())
}

/// Removes a user form the service, first authenticates than checks if they are subscribed
pub async fn unsubscribe(
    State(state): State<Arc<Mutex<AppState<impl AsyncTransport, impl ConnectionTrait>>>>,
    Json(user_auth): Json<UserAuth>,
) -> Result<()> {
    let mut app_state = state.lock().await;
    user_auth.auth(&mut app_state.otp_db).await?;

    Ok(
        match get_user_by_email(&app_state.db, &user_auth.email.to_string()).await? {
            Some(user) => delete_user(user, &app_state.db).await?,
            None => {}
        },
    )
}

/// The User and the list of stations that they want to be subscribed to
#[derive(Deserialize)]
pub struct Subscription {
    user_auth: UserAuth,
    stations: Vec<String>,
}

// The reason for why there are so many subfunctions for [`update_user_stations`] was to chase down a bug with unit testing. After that I figured I would leave it.

/// Fetches the station models from the list of names given
async fn get_stations_from_names(
    db: &impl ConnectionTrait,
    names: &[String],
) -> Result<Vec<stations::Model>> {
    Ok(Stations::find()
        .all(db)
        .await?
        .into_iter()
        .filter(|station| names.contains(&station.name))
        .collect())
}

/// Gets the stations that the user is already subscribed to
async fn get_already_selected_stations(
    user: &users::Model,
    db: &impl ConnectionTrait,
) -> Result<Vec<stations::Model>> {
    Ok(user.find_related(Stations).all(db).await?)
}

/// Deletes from the database any links to stations not in the list
async fn delete_user_stations_not_in_list(
    user: &users::Model,
    db: &impl ConnectionTrait,
    stations: &[stations::Model],
) -> Result<()> {
    let mut user_stations = user.find_related(UserStations).all(db).await?;
    user_stations.retain(|user_station| {
        !stations
            .iter()
            .any(|station| user_station.station_id == station.id)
    });

    // Unfortunately I cannot choose specific records to delete like I can with INSERT, so I am doing them one at a time.
    for user_station in user_stations {
        user_station.delete(db).await?;
    }
    Ok(())
}

/// Removes from [`stations`] any that are in [`already_selected_stations`]
fn filter_out_stations(
    stations: &mut Vec<stations::Model>,
    already_selected_stations: &[stations::Model],
) {
    stations.retain(|station| !already_selected_stations.contains(station));
}

/// Removes stations from the list that the user is already subscribed to
async fn remove_already_selected_stations(
    user: &users::Model,
    db: &impl ConnectionTrait,
    stations: &mut Vec<stations::Model>,
) -> Result<()> {
    let already_selected_stations = get_already_selected_stations(user, db).await?;
    filter_out_stations(stations, &already_selected_stations);
    Ok(())
}

/// Deletes links from the database that are not in the list, than removes stations that already have links from the list. Order is important
async fn delete_not_selected_user_stations(
    user: &users::Model,
    db: &impl ConnectionTrait,
    stations: &mut Vec<stations::Model>,
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

    let user = match get_user_by_email(db, &email).await? {
        Some(user) => {
            delete_not_selected_user_stations(&user, db, &mut stations).await?;
            user
        }
        None => {
            users::ActiveModel {
                email: ActiveValue::Set(email),
                ..Default::default()
            }
            .insert(db)
            .await?
        }
    };

    UserStations::insert_many(
        stations
            .into_iter()
            .map(|station| user_stations::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }),
    )
    .on_empty_do_nothing()
    .exec_without_returning(db)
    .await?;
    Ok(())
}

/// Wrapper for [`update_user_stations`] that includes authentication
pub async fn update_subscription<T: AsyncTransport, C: ConnectionTrait>(
    State(state): State<Arc<Mutex<AppState<T, C>>>>,
    Json(subscription): Json<Subscription>,
) -> Result<()> {
    let mut app_state = state.lock().await;
    subscription.user_auth.auth(&mut app_state.otp_db).await?; // Will error out here if auth fails

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
        database::{prelude::*, stations, user_stations, users},
        update_user_stations,
    };

    /// Creates a in-memory SQLite database with dummy stations (taken from the actual WMATA network) for testing
    async fn setup_test_db() -> Result<sea_orm::DatabaseConnection, sea_orm::DbErr> {
        use sea_orm::ConnectionTrait;

        let db = sea_orm::Database::connect("sqlite::memory:").await?;
        let backend = db.get_database_backend();
        let schema = sea_orm::Schema::new(backend);

        let table_create_statements = [
            schema.create_table_from_entity(RailLines),
            schema.create_table_from_entity(Users),
            schema.create_table_from_entity(Stations),
            schema.create_table_from_entity(UserStations),
        ];
        for statement in table_create_statements {
            db.execute(backend.build(&statement)).await?;
        }

        let stations = [
            stations::ActiveModel {
                id: ActiveValue::Set(44),
                name: ActiveValue::Set(String::from("Herndon")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(69),
                name: ActiveValue::Set(String::from("Reston Town Center")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(97),
                name: ActiveValue::Set(String::from("Wiehle-Reston East")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(79),
                name: ActiveValue::Set(String::from("Spring Hill")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(42),
                name: ActiveValue::Set(String::from("Greensboro")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(85),
                name: ActiveValue::Set(String::from("Tysons")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(53),
                name: ActiveValue::Set(String::from("McLean")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(26),
                name: ActiveValue::Set(String::from("East Falls Church")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(6),
                name: ActiveValue::Set(String::from("Ballston-MU")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(91),
                name: ActiveValue::Set(String::from("Virginia Square-GMU")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(15),
                name: ActiveValue::Set(String::from("Clarendon")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(20),
                name: ActiveValue::Set(String::from("Court House")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(73),
                name: ActiveValue::Set(String::from("Rosslyn")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(33),
                name: ActiveValue::Set(String::from("Foggy Bottom-GWU")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(30),
                name: ActiveValue::Set(String::from("Farragut West")),
            },
            stations::ActiveModel {
                id: ActiveValue::Set(54),
                name: ActiveValue::Set(String::from("McPherson Square")),
            },
        ];

        Stations::insert_many(stations)
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

        let user = users::ActiveModel {
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
        let user = users::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();
        UserStations::insert_many([6, 15, 26, 42, 44, 53, 69, 79, 85, 91, 97].map(|id| {
            user_stations::ActiveModel {
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
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];
        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn get_already_selected_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = users::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = [
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        let user_stations: Vec<_> = stations
            .iter()
            .map(|station| user_stations::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            })
            .collect();
        UserStations::insert_many(user_stations)
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
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
        ];

        let mut stations = vec![
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        super::filter_out_stations(&mut stations, &already_selected_stations);

        let expected = [
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn remove_already_selected_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = users::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let already_selected_stations = [
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
        ];

        UserStations::insert_many(already_selected_stations.map(|station| {
            user_stations::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }
        }))
        .on_empty_do_nothing()
        .exec_without_returning(&db)
        .await
        .unwrap();

        let mut stations = vec![
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        super::remove_already_selected_stations(&user, &db, &mut stations)
            .await
            .unwrap();

        let expected = [
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        assert_eq!(stations, expected);
    }

    #[tokio::test]
    async fn delete_user_stations_not_in_list_test() {
        let db = setup_test_db().await.unwrap();
        let user = users::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = vec![
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        let user_stations: Vec<_> = stations
            .iter()
            .map(|station| user_stations::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            })
            .collect();
        UserStations::insert_many(user_stations)
            .on_conflict_do_nothing()
            .exec_without_returning(&db)
            .await
            .unwrap();

        let expected = [
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
        ];

        super::delete_user_stations_not_in_list(&user, &db, &expected)
            .await
            .unwrap();

        let result = user.find_related(Stations).all(&db).await.unwrap();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn delete_not_selected_user_stations_test() {
        let db = setup_test_db().await.unwrap();
        let user = users::ActiveModel::default_values()
            .insert(&db)
            .await
            .unwrap();

        let stations = [
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
        ];

        UserStations::insert_many(stations.map(|station| user_stations::ActiveModel {
            user_id: ActiveValue::Set(user.id),
            station_id: ActiveValue::Set(station.id),
        }))
        .on_conflict_do_nothing()
        .exec_without_returning(&db)
        .await
        .unwrap();

        let mut selected_stations = vec![
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
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

        let current_stations = user.find_related(Stations).all(&db).await.unwrap();
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

        let user_stations = Users::find_by_id(1)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .find_related(UserStations)
            .all(&db)
            .await
            .unwrap();
        let expected =
            [6, 15, 26, 42, 44, 53, 69, 79, 85, 91, 97].map(|stations_id| user_stations::Model {
                user_id: 1,
                station_id: stations_id,
            });
        assert_eq!(user_stations, expected);
    }

    async fn update_user_stations_existing_user_test_framework(
        starting_stations: &[stations::Model],
        stations: &[String],
    ) -> Result<Vec<stations::Model>, Error> {
        let db = setup_test_db().await?;
        let user = users::ActiveModel::default_values().insert(&db).await?;
        UserStations::insert_many(starting_stations.into_iter().map(|station| {
            user_stations::ActiveModel {
                user_id: ActiveValue::Set(user.id),
                station_id: ActiveValue::Set(station.id),
            }
        }))
        .on_conflict_do_nothing()
        .exec_without_returning(&db)
        .await?;

        update_user_stations(&db, &stations, user.email.clone()).await?;
        Ok(user.find_related(Stations).all(&db).await?)
    }

    #[tokio::test]
    async fn update_user_stations_test_add_stations() {
        let starting_stations = [
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
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
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
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
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
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
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
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
            stations::Model {
                id: 44,
                name: String::from("Herndon"),
            },
            stations::Model {
                id: 69,
                name: String::from("Reston Town Center"),
            },
            stations::Model {
                id: 97,
                name: String::from("Wiehle-Reston East"),
            },
            stations::Model {
                id: 79,
                name: String::from("Spring Hill"),
            },
            stations::Model {
                id: 42,
                name: String::from("Greensboro"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 91,
                name: String::from("Virginia Square-GMU"),
            },
            stations::Model {
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
            stations::Model {
                id: 6,
                name: String::from("Ballston-MU"),
            },
            stations::Model {
                id: 15,
                name: String::from("Clarendon"),
            },
            stations::Model {
                id: 20,
                name: String::from("Court House"),
            },
            stations::Model {
                id: 26,
                name: String::from("East Falls Church"),
            },
            stations::Model {
                id: 30,
                name: String::from("Farragut West"),
            },
            stations::Model {
                id: 33,
                name: String::from("Foggy Bottom-GWU"),
            },
            stations::Model {
                id: 53,
                name: String::from("McLean"),
            },
            stations::Model {
                id: 54,
                name: String::from("McPherson Square"),
            },
            stations::Model {
                id: 73,
                name: String::from("Rosslyn"),
            },
            stations::Model {
                id: 85,
                name: String::from("Tysons"),
            },
            stations::Model {
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

    #[tokio::test]
    async fn transport_test_connection() {
        use std::env;

        assert!(
            lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::starttls_relay(
                &env::var("RELAY").unwrap(),
            )
            .unwrap()
            .credentials(lettre::transport::smtp::authentication::Credentials::new(
                env::var("NAME").unwrap_or_else(|_| env::var("ADDRESS").unwrap()),
                env::var("PASSWORD").unwrap(),
            ))
            .build::<lettre::Tokio1Executor>()
            .test_connection()
            .await
            .unwrap()
        )
    }
}
