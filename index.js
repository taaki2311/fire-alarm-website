/**
 * @param {string} message - Message to give to the user
 */
function displayResult(message) {
    document.getElementById('result').innerText = message;
}

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
    toHtml() {
        return `<dt name="${this.name}"><input type="checkbox" id="${this.name}"><label for="${this.name}">${this.name}</label>
            </dt><dd name="${this.name}"><ul>`
            // eslint-disable-next-line no-undef
            + this.lines.map((line) => `<li style="background-color: ${colors.get(line).toRgb()};">${line}</li>`).join('')
            + `</ul></dd>`;
    }
}

/**
 * Initializes the list of all the stations with each line they are on
 */
let stations;
async function getStationInfos() {
    try {
        const json = await fetch('/get_stations').then((response) => response.json());
        stations = json.map((station) => new StationInfo(station.name, station.lines));
        document.getElementById('stationList').innerHTML = stations.map((station) => station.toHtml()).join('');
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
            const elements = Array.from(document.getElementsByName(station.name));
            if (station.shouldDisplay(filter)) {
                elements.map((element) => element.style.display = 'contents');
            } else {
                elements.map((element) => element.style.display = 'none');
            }
        }
    } else {
        for (const station of stations) {
            Array.from(document.getElementsByName(station.name)).map((element) => element.style.display = 'contents');
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

document.getElementById('subscriptionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = new FormData(document.getElementById('emailForm')).get('email');
    const code = parseInt(new FormData(event.target).get('code'));
    const user_auth = new UserAuth(email, code);
    const stations = Array.from(document.getElementById('stationList').getElementsByTagName('input'))
        .filter((element) => 'checkbox' === element.type && element.checked).map((station) => station.id);

    let init;
    if (0 === stations.length) {
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