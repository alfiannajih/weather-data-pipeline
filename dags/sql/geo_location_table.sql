CREATE TABLE IF NOT EXISTS geo_location (
    "id" INTEGER,
    "province" TEXT,
    "city" TEXT,
    "district" TEXT,
    "longitude" FLOAT,
    "latitude" FLOAT,
    "timezone" TEXT,
    PRIMARY KEY ("id")
);