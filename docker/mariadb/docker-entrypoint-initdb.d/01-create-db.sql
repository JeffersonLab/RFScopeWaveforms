CREATE DATABASE scope_waveforms;

USE scope_waveforms;
CREATE TABLE scan (
    sid INT AUTO_INCREMENT,
    scan_start_utc DATETIME NOT NULL,
    PRIMARY KEY(sid)
);
CREATE INDEX scan_start_index ON scan  (scan_start_utc);

CREATE TABLE waveform (
    wid INT AUTO_INCREMENT,
    sid INT,
    cavity varchar(16) NOT NULL,
    signal_name varchar(16) NOT NULL,
    comment VARCHAR(2048),
    PRIMARY KEY (wid),
    FOREIGN KEY (sid) REFERENCES scan(sid)
);
CREATE INDEX waveform_cavity_index on waveform (cavity);

CREATE TABLE waveform_adata (
  wadid int AUTO_INCREMENT,
  wid int,
  name varchar(16) NOT NULL,  # Name of the array (raw, frequencies, power_spectrum)
  data JSON NOT NULL,         # Array data in a json object
  PRIMARY KEY (wadid),
  FOREIGN KEY (wid) REFERENCES waveform(wid)
);
CREATE INDEX wad_name_index on waveform_adata (name);

CREATE TABLE waveform_sdata (
  wadid int AUTO_INCREMENT,
  wid int,
  name varchar(16) NOT NULL,  # Name of the scalar metric (mean, rms, etc.)
  value FLOAT NOT NULL,       # Value of the scalar metric
  PRIMARY KEY (wadid),
  FOREIGN KEY (wid) REFERENCES waveform(wid)
);
CREATE INDEX wad_name_index on waveform_adata (name);

CREATE TABLE scan_fdata (
  sfid int AUTO_INCREMENT,
  sid int,
  name varchar(32) NOT NULL,  # Name of the metric
  value FLOAT NOT NULL,       # Value of the metric
  PRIMARY KEY (sfid),
  FOREIGN KEY (sid) REFERENCES scan(sid)
);
CREATE INDEX sf_name_index on scan_fdata (name);
CREATE INDEX sf_value_index on scan_fdata (value);

CREATE TABLE scan_sdata (
  wsdid int AUTO_INCREMENT,
  wid int,
  name varchar(32) NOT NULL,
  value varchar(512) NOT NULL,
  PRIMARY KEY (wsdid),
  FOREIGN KEY (wid) REFERENCES waveform(wid)
);
CREATE INDEX ss_name_index on scan_sdata (name);
CREATE INDEX ss_value_index on scan_sdata (name);
