CREATE TABLE IF NOT EXISTS product (
    asin varchar(15),
    title text,
    price float,
    description text,
    id_category int,
    id_brand int,
    PRIMARY KEY(asin, id_category)
);