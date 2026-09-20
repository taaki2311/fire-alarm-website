BEGIN;
CREATE TABLE IF NOT EXISTS RailLines (
    id INTEGER PRIMARY KEY NOT NULL UNIQUE,
    name VARCHAR(16) NOT NULL UNIQUE,
    -- Rail lines typically have colors associated with them, these are for the RGB value
    red SMALLINT NOT NULL,
    green SMALLINT NOT NULL,
    blue SMALLINT NOT NULL
);
CREATE TABLE IF NOT EXISTS Stations (
    id INTEGER PRIMARY KEY NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS LineStations (
    line_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    FOREIGN KEY (line_id) REFERENCES RailLines(id),
    FOREIGN KEY (station_id) REFERENCES Stations(id),
    PRIMARY KEY (line_id, station_id)
);
CREATE TABLE IF NOT EXISTS Users (
    id INTEGER PRIMARY KEY NOT NULL UNIQUE,
    -- Maximum length of an email address from https://datatracker.ietf.org/doc/html/rfc5321#section-4.5.3.1
    email VARCHAR(320) NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS UserStations (
    user_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES Users(id),
    FOREIGN KEY (station_id) REFERENCES Stations(id),
    PRIMARY KEY (user_id, station_id)
);

INSERT INTO RailLines VALUES (1, 'Ruby', 255, 0, 0);
INSERT INTO RailLines VALUES (2, 'Emerald', 0, 255, 0);
INSERT INTO RailLines VALUES (3, 'Sapphire', 0, 0, 255);

INSERT INTO Stations VALUES (1, 'Foo');
INSERT INTO LineStations VALUES (1, 1);
INSERT INTO Stations VALUES (2, 'Bar');
INSERT INTO LineStations VALUES (1, 2);
INSERT INTO LineStations VALUES (2, 2);
INSERT INTO Stations VALUES (3, 'Baz');
INSERT INTO LineStations VALUES (2, 3);
INSERT INTO LineStations VALUES (3, 3);
COMMIT;