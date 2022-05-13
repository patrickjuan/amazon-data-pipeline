CREATE TABLE IF NOT EXISTS reviews
(
    reviewerid     varchar(30),
    asin           varchar(15),
    reviewername   text,
    helpful        text,
    reviewtext     text,
    overall        double precision,
    summary        text,
    unixreviewtime bigint,
    reviewtime     text
);