class StationInfo {
    /**
     * @param {string} name - Name of the station
     * @param {string[]} lines - Lines that the station is on
     */
    constructor(name, lines) {
        this.name = name;
        this.lines = lines;
    }

    /**
     * @param {Map<string, boolean>} filter - Filter to be checked against
     * @returns {boolean} - Should the station be displayed?
     */
    shouldDisplay(filter) {
        for (const line of this.lines) {
            if (filter.get(line)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @returns {string} HTML formatting for a checkbox
     */
    toCheckbox() {
        return `<div id="${this.name}"><label><input type="checkbox" name="${this.name}">${this.name}</label></div>`;
    }
}

/**
 * @param {string} message - Message to give to the user
 */
function displayResult(message) {
    document.getElementById('result').innerText = message;
}

/**
 * Initializes the filter for all the metro lines
 */
let filter; // eslint-disable-line no-unused-vars
async function getLines() {
    try {
        const lines = await fetch('/get_lines').then((response) => response.json());
        document.getElementById('stationsFilter').innerHTML = lines.map((line) => `<label>
                    <input type="checkbox" name="${line}" onchange="updateStationList(filter, stations, this.name, this.checked)">
                    ${line}</label>`).join('');
        filter = new Map(lines.map((line) => [line, false]));
    } catch (error) {
        displayResult(`Error when getting lines: ${error}`);
    }
}
getLines();

/**
 * Initializes the list of all the stations with each line they are on
 */
let stations;
async function getStationInfos() {
    try {
        const json = await fetch('/get_stations').then((response) => response.json());
        stations = json.map(station => new StationInfo(station.name, station.lines));
        document.getElementById('stationList').innerHTML = stations.map((station) => station.toCheckbox()).join('');
    } catch (error) {
        displayResult(`Error when getting station information: ${error}`);
    }
}
getStationInfos();

/**
 * @param {Map<string, boolean>} filter - Filter to be updated
 * @param {StationInfo[]} stations - Stations to be filtered
 * @param {string} name - Name of the line toggled
 * @param {boolean} checked - State of the checkbox
 */
function updateStationList(filter, stations, name, checked) { // eslint-disable-line no-unused-vars
    filter.set(name, checked);
    if (filter.values().some((enabled) => enabled)) {
        for (const station of stations) {
            const element = document.getElementById(station.name).style;
            if (station.shouldDisplay(filter)) {
                element.display = 'block';
            } else {
                element.display = 'none';
            }
        }
    } else {
        for (const station of stations) {
            document.getElementById(station.name).style.display = 'block';
        }
    }
}

document.getElementById('emailForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = new FormData(event.target).get('email');

    try {
        const response = await fetch('/submit_email', {
            method: 'POST',
            body: email
        });

        if (response.ok) {
            document.getElementById('subscriptionForm').style.display = 'block';
            displayResult('Email Successfully received');
        } else {
            displayResult(`Error receiving email: ${await response.text()}`);
        }
    } catch (error) {
        displayResult(`Error submitting email: ${error}`);
    }
});

class UserAuth {
    /**
     * @param {string} email - Email Address of the submission
     * @param {number} code - One Time Passcode used for verification
     */
    constructor(email, code) {
        this.email = email;
        this.code = code;
    }
}

class Subscription {
    /**
     * @param {UserAuth} user_auth - Information (email and one-time passcode) used to authenticate the user
     * @param {sting[]} stations - Stations that the user wants to subscribe to
     */
    constructor(user_auth, stations) {
        this.user_auth = user_auth;
        this.stations = stations;
    }
}

/**
 * @returns {string[]} - List of the stations that the user has selected
 */
function getStations() {
    return Array.from(document.getElementById('stationList').elements)
        .filter((station) => station.checked).map((station) => station.getAttribute('name'));
}

document.getElementById('subscriptionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = new FormData(document.getElementById('emailForm')).get('email');
    const code = parseInt(new FormData(event.target).get('code'));
    const user_auth = new UserAuth(email, code);
    const stations = getStations();

    let init;
    if (stations.length === 0) {
        init = {
            method: 'DELETE',
            body: JSON.stringify(user_auth),
            headers: { 'Content-type': 'application/json' }
        };
    } else {
        init = {
            method: 'PUT',
            body: JSON.stringify(new Subscription(user_auth, stations)),
            headers: { 'Content-type': 'application/json' }
        };
    }

    try {
        const response = await fetch('/update_subscription', init);
        if (response.ok) {
            displayResult('Verification code authenticated successfully');
        } else {
            displayResult(`Error verifying code: ${await response.text()}`);
        }
    } catch (error) {
        displayResult(`Error sending code: ${error}`);
    }
});