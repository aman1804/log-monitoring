CREATE TABLE access_logs (

    id SERIAL PRIMARY KEY,

    ip VARCHAR(50),

    timestamp TIMESTAMP,

    method VARCHAR(10),

    endpoint TEXT,

    protocol VARCHAR(20),

    status INT,

    bytes INT,

    referer TEXT,

    user_agent TEXT

);


CREATE TABLE error_logs (

    id SERIAL PRIMARY KEY,

    ip VARCHAR(50),

    timestamp TIMESTAMP,

    method VARCHAR(10),

    endpoint TEXT,

    protocol VARCHAR(20),

    status INT,

    error_type VARCHAR(50),

    user_agent TEXT

);


CREATE TABLE alerts (

    id SERIAL PRIMARY KEY,

    alert_type VARCHAR(100),

    severity VARCHAR(20),

    description TEXT,

    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE top_endpoints (

    id SERIAL PRIMARY KEY,

    endpoint TEXT,

    request_count INT,

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);


CREATE TABLE top_ips (

    id SERIAL PRIMARY KEY,

    ip VARCHAR(50),

    request_count INT,

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE requests_per_minute (

    id SERIAL PRIMARY KEY,

    window_start TIMESTAMP,

    window_end TIMESTAMP,

    request_count INT

);


CREATE TABLE errors_per_minute (

    id SERIAL PRIMARY KEY,

    window_start TIMESTAMP,

    window_end TIMESTAMP,

    error_count INT

);