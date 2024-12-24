CREATE DATABASE CabDatabase;
GO

USE CabDatabase;
GO;

CREATE TABLE CabTripData
(
    Id                  BIGINT IDENTITY (1,1) PRIMARY KEY,
    PickupDateTime      DATETIME2      NOT NULL,
    DropoffDateTime     DATETIME2      NOT NULL,
    PassengerCount      INT,
    TripDistance        DECIMAL(10, 2) NOT NULL,
    StoreAndFwdFlag     NVARCHAR(3)    NOT NULL,
    PULocationID        INT            NOT NULL,
    DOLocationID        INT            NOT NULL,
    FareAmount          DECIMAL(10, 2) NOT NULL,
    TipAmount           DECIMAL(10, 2) NOT NULL,
    TripDurationMinutes AS DATEDIFF(MINUTE, PickupDateTime, DropoffDateTime)
);

CREATE NONCLUSTERED INDEX IX_PULocationID_TipAmount ON CabTripData (PULocationID, TipAmount);
CREATE NONCLUSTERED INDEX IX_TripDistance ON CabTripData (TripDistance DESC);
CREATE NONCLUSTERED INDEX IX_TripDuration ON CabTripData (TripDurationMinutes DESC);