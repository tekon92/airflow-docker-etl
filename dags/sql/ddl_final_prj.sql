create table dim_case
(
    status_id     int,
    status_name   varchar(255),
    status_detail varchar(255)
);


create table dim_province
(
    province_id   int,
    province_name varchar(255)
);

create table dim_district
(
    district_id   int,
    province_id   int,
    district_name varchar(255)
);


create table fact_province_district_dly
(
    pk_id       serial primary key,
    province_id int,
    district_id int,
    tanggal     date,
    case_id     int,
    total       bigint
);

create table fact_province_dly
(
    pk_id       serial primary key,
    tanggal     date,
    province_id int,
    case_id     int,
    total       bigint
);

create table fact_province_mth
(
    pk_id       serial primary key,
    province_id int,
    month_sk_id int,
    case_id     int,
    total       bigint
);

create table fact_province_year
(
    pk_id       serial primary key,
    province_id int,
    year_sk_id  int,
    case_id     int,
    total       bigint
);

create table fact_district_dly
(
    pk_id       serial primary key,
    tanggal     date,
    district_id int,
    case_id     int,
    total       bigint
);

create table fact_district_mth
(
    pk_id       serial primary key,
    district_id int,
    month_sk_id int,
    case_id     int,
    total       bigint
);

create table fact_district_yearly
(
    pk_id       serial primary key,
    district_id int,
    year_sk_id  int,
    case_id     int,
    total        bigint
);
