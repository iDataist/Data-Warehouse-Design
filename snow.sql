-- login
snowsql -a JNA30169 -u dataist

-- split the large Json files into smaller pieces
wc -l yelp_academic_dataset_user.json
split -l 350000 yelp_academic_dataset_user.json

wc -l yelp_academic_dataset_review.json
split -l 700000 yelp_academic_dataset_review.json

-- Create a database
use role sysadmin;
use warehouse compute_wh;

create database review;
use database review;

-- Create a staging environment(schema)
create schema staging;
use schema staging;

-- Upload data to the staging environment (screenshot 1 and 2)
create
or replace file format csv_format type = csv FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true compression = gzip error_on_column_count_mismatch = false;

create
or replace stage my_csv_stage file_format = csv_format;

create
or replace table temperature(
    date string not null,
    min float,
    max float,
    min_normal float,
    max_normal float
);
put file:///Users/huiren/Downloads/USC00362574-temperature-degreeF.csv @my_csv_stage auto_compress=true;
copy into temperature from @my_csv_stage/USC00362574-temperature-degreeF.csv.gz file_format = (format_name = csv_format) on_error = 'skip_file';

create
or replace table precipitation(
    date string not null,
    precipitation float,
    precipitation_normal float
);
put file:///Users/huiren/Downloads/USC00362574-EMSWORTH_L_D_OHIO_RIVER-precipitation-inch.csv @my_csv_stage auto_compress=true;
copy into precipitation from @my_csv_stage/USC00362574-EMSWORTH_L_D_OHIO_RIVER-precipitation-inch.csv.gz file_format = (format_name = csv_format) on_error = 'skip_file';

create
or replace file format json_format type = 'JSON' strip_outer_array = true;

create
or replace stage my_json_stage file_format = json_format;

create table covid_features(v variant);
put file:///Users/huiren/Downloads/yelp_academic_dataset_covid_features.json @my_json_stage auto_compress=true;
copy into covid_features from @my_json_stage/yelp_academic_dataset_covid_features.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

create table business(v variant);
put file:///Users/huiren/Downloads/yelp_academic_dataset_business.json @my_json_stage auto_compress=true;
copy into business from @my_json_stage/yelp_academic_dataset_business.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

create table tip(v variant);
put file:///Users/huiren/Downloads/yelp_academic_dataset_tip.json @my_json_stage auto_compress=true;
copy into tip from @my_json_stage/yelp_academic_dataset_tip.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

create table checkin(v variant);
put file:///Users/huiren/Downloads/yelp_academic_dataset_checkin.json @my_json_stage auto_compress=true;
copy into checkin from @my_json_stage/yelp_academic_dataset_checkin.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

create table user(v variant);

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_1.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_1.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_2.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_2.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_3.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_3.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_4.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_4.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_5.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_5.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_user_6.json @my_json_stage auto_compress=true;
copy into user from @my_json_stage/yelp_academic_dataset_user_6.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

create table review(v variant);

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_1.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_1.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_2.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_2.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_3.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_3.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_4.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_4.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_5.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_5.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_6.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_6.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_7.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_7.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_8.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_8.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_9.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_9.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_10.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_10.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_11.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_11.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

put file:///Users/huiren/Downloads/yelp_academic_dataset_review_12.json @my_json_stage auto_compress=true;
copy into review from @my_json_stage/yelp_academic_dataset_review_12.json.gz file_format = (format_name = json_format) on_error = 'skip_file';

-- Create an ODS environment(schema)
create schema ODS;
use schema ODS;

-- Migrate the data into the ODS environment
create
or replace sequence seq start = 1 increment = 1;

create
or replace table temperature(
    temperature_id int,
    date_recorded date,
    min_recorded float,
    max_recorded float,
    min_normal float,
    max_normal float,
    constraint pk_temperature_id primary key (temperature_id)
);

insert into
    temperature
select
    seq.nextval,
    to_date(date, 'YYYYMMDD'),
    min,
    max,
    min_normal,
    max_normal
from
    staging.temperature;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table precipitation(
    precipitation_id int,
    date_recorded date,
    precipitation float,
    precipitation_normal float,
    constraint pk_precipitation_id primary key (precipitation_id)
);

insert into
    precipitation
select
    seq.nextval,
    to_date(date, 'YYYYMMDD'),
    precipitation,
    precipitation_normal
from
    staging.precipitation;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table covid_features(
    covid_id int,
    business_id string,
    highlights string,
    delivery_or_takeout string,
    grubhub_enabled string,
    call_to_action_enabled string,
    request_a_quote_enabled string,
    covid_banner string,
    temporary_closed_until string,
    virtual_services_offered string,
    constraint pk_covid_id primary key (covid_id)
);

insert into
    covid_features
select
    seq.nextval,
    v :business_id :: string,
    v :highlights :: string,
    v :"delivery or takeout" :: string,
    v :"Grubhub enabled" :: string,
    v :"Call To Action enabled" :: string,
    v :"Request a Quote Enabled" :: string,
    v :"Covid Banner" :: string,
    v :"Temporary Closed Until" :: string,
    v :"Virtual Services Offered" :: string
from
    staging.covid_features;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table business(
    id int,
    business_id string,
    name string,
    address string,
    city string,
    state string,
    postal_code string,
    latitude float,
    longitude float,
    stars float,
    review_count int,
    is_open int,
    attributes string,
    categories string,
    hours string,
    constraint pk_id primary key (id)
);

insert into
    business
select
    seq.nextval,
    v :business_id :: string,
    v :name :: string,
    v :address :: string,
    v :city :: string,
    v :state :: string,
    v :postal_code :: string,
    v :latitude :: float,
    v :longitude :: float,
    v :stars :: float,
    v :review_count :: int,
    v :is_open :: int,
    v :attributes :: string,
    v :categories :: string,
    v :hours :: string
from
    staging.business;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table tip(
    tip_id int,
    user_id string,
    business_id string,
    text string,
    date timestamp,
    compliment_count int,
    constraint pk_tip_id primary key (tip_id)
);

insert into
    tip
select
    seq.nextval,
    v :user_id :: string,
    v :business_id :: string,
    v :text :: string,
    v :date :: timestamp,
    v :compliment_count :: int
from
    staging.tip;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table checkin(
    checkin_id int,
    business_id string,
    date string,
    constraint pk_checkin_id primary key (checkin_id)
);

insert into
    checkin
select
    seq.nextval,
    v :business_id :: string,
    v :date :: string
from
    staging.checkin;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table user(
    id int,
    user_id string,
    name string,
    review_count int,
    yelping_since string,
    useful int,
    funny int,
    cool int,
    elite string,
    friends string,
    fans int,
    average_stars float,
    compliment_hot int,
    compliment_more int,
    compliment_profile int,
    compliment_cute int,
    compliment_list int,
    compliment_note int,
    compliment_plain int,
    compliment_cool int,
    compliment_funny int,
    compliment_writer int,
    compliment_photos int,
    constraint pk_id primary key (id)
);

insert into
    user
select
    seq.nextval,
    v :user_id :: string,
    v :name :: string,
    v :review_count :: int,
    v :yelping_since :: string,
    v :useful :: int,
    v :funny :: int,
    v :cool :: int,
    v :elite :: string,
    v :friends :: string,
    v :fans :: int,
    v :average_stars :: float,
    v :compliment_hot :: int,
    v :compliment_more :: int,
    v :compliment_profile :: int,
    v :compliment_cute :: int,
    v :compliment_list :: int,
    v :compliment_note :: int,
    v :compliment_plain :: int,
    v :compliment_cool :: int,
    v :compliment_funny :: int,
    v :compliment_writer :: int,
    v :compliment_photos :: int
from
    staging.user;

create
or replace sequence seq start = 1 increment = 1;

create
or replace table review(
    id int,
    review_id string,
    user_id string,
    business_id string,
    stars int,
    useful int,
    funny int,
    cool int,
    text string,
    date timestamp,
    constraint pk_id primary key (id)
);

insert into
    review
select
    seq.nextval,
    v :review_id :: string,
    v :user_id :: string,
    v :business_id :: string,
    v :stars :: int,
    v :useful :: int,
    v :funny :: int,
    v :cool :: int,
    v :text :: string,
    v :date :: timestamp
from
    staging.review;

-- Create a view to integrate climate and Yelp data
create
or replace view climate_yelp as
select
    b.name,
    t.min_recorded,
    t.max_recorded,
    p.precipitation,
    r.stars
from
    review r
    join business b on r.business_id = b.business_id
    join temperature t on to_date(r.date) = t.date_recorded
    join precipitation p on to_date(r.date) = p.date_recorded
where
    b.city = 'Pittsburgh'
    and b.state = 'PA';

-- Create a DWH environment(aka schema)
create schema DWH;
use schema DWH;

-- Migrate the data into the DWH environment
create
or replace sequence seq start = 1 increment = 1;

create
or replace table fact(
    id int,
    review_dimention_id int,
    user_dimention_id int,
    business_dimention_id int,
    tip_id int,
    checkin_id int,
    covid_id int,
    temperature_id int,
    precipitation_id int,
    date timestamp,
    constraint pk_id primary key (id)
);

insert into
    fact
select
    seq.nextval,
    r.id,
    u.id,
    b.id,
    t.tip_id,
    c.checkin_id,
    cf.covid_id,
    te.temperature_id,
    p.precipitation_id,
    r.date
from
    ods.review r full
    join ods.user u on r.user_id = u.user_id full
    join ods.business b on r.business_id = b.business_id full
    join ods.tip t on r.user_id = t.user_id
    and to_date(r.date) = to_date(t.date) full
    join ods.checkin c on r.business_id = c.business_id full
    join ods.covid_features cf on r.business_id = cf.business_id full
    join ods.temperature te on to_date(r.date) = te.date_recorded full
    join ods.precipitation p on to_date(r.date) = p.date_recorded;

create
or replace table temperature_dimention(
    temperature_id int,
    date_recorded date,
    min_recorded float,
    max_recorded float,
    min_normal float,
    max_normal float,
    constraint pk_temperature_id primary key (temperature_id)
);

insert into
    temperature_dimention
select
    *
from
    ods.temperature;

create
or replace table precipitation_dimention(
    precipitation_id int,
    date_recorded date,
    precipitation float,
    precipitation_normal float,
    constraint pk_precipitation_id primary key (precipitation_id)
);

insert into
    precipitation_dimention
select
    *
from
    ods.precipitation;

create
or replace table covid_features_dimention(
    covid_id int,
    business_id string,
    highlights string,
    delivery_or_takeout string,
    grubhub_enabled string,
    call_to_action_enabled string,
    request_a_quote_enabled string,
    covid_banner string,
    temporary_closed_until string,
    virtual_services_offered string,
    constraint pk_covid_id primary key (covid_id)
);

insert into
    covid_features_dimention
select
    *
from
    ods.covid_features;

create
or replace table business_dimention(
    id int,
    business_id string,
    name string,
    address string,
    city string,
    state string,
    postal_code string,
    latitude float,
    longitude float,
    stars float,
    review_count int,
    is_open int,
    attributes string,
    categories string,
    hours string,
    constraint pk_id primary key (id)
);

insert into
    business_dimention
select
    *
from
    ods.business;

create
or replace table tip_dimention(
    tip_id int,
    user_id string,
    business_id string,
    text string,
    date timestamp,
    compliment_count int,
    constraint pk_tip_id primary key (tip_id)
);

insert into
    tip_dimention
select
    *
from
    ods.tip;

create
or replace table checkin_dimention(
    checkin_id int,
    business_id string,
    date string,
    constraint pk_checkin_id primary key (checkin_id)
);

insert into
    checkin_dimention
select
    *
from
    ods.checkin;

create
or replace table user_dimention(
    id int,
    user_id string,
    name string,
    review_count int,
    yelping_since string,
    useful int,
    funny int,
    cool int,
    elite string,
    friends string,
    fans int,
    average_stars float,
    compliment_hot int,
    compliment_more int,
    compliment_profile int,
    compliment_cute int,
    compliment_list int,
    compliment_note int,
    compliment_plain int,
    compliment_cool int,
    compliment_funny int,
    compliment_writer int,
    compliment_photos int,
    constraint pk_id primary key (id)
);

insert into
    user_dimention
select
    *
from
    ods.user;

create
or replace table review_dimention(
    id int,
    review_id string,
    user_id string,
    business_id string,
    stars int,
    useful int,
    funny int,
    cool int,
    text string,
    date timestamp,
    constraint pk_id primary key (id)
);

insert into
    review_dimention
select
    *
from
    ods.review;

alter table
    fact
add
    constraint fkey_temperature_id foreign key (temperature_id) references temperature_dimention (temperature_id);

alter table
    fact
add
    constraint fkey_precipitation_id foreign key (precipitation_id) references precipitation_dimention (precipitation_id);

alter table
    fact
add
    constraint fkey_covid_id foreign key (covid_id) references covid_features_dimention (covid_id);

alter table
    fact
add
    constraint fkey_business_id foreign key (business_dimention_id) references business_dimention (id);

alter table
    fact
add
    constraint fkey_tip_id foreign key (tip_id) references tip_dimention (tip_id);

alter table
    fact
add
    constraint fkey_checkin_id foreign key (checkin_id) references checkin_dimention (checkin_id);

alter table
    fact
add
    constraint fkey_user_id foreign key (user_dimention_id) references user_dimention (id);

alter table
    fact
add
    constraint fkey_review_id foreign key (review_dimention_id) references review_dimention (id);

-- Reports the business name, temperature, precipitation, and ratings
create
or replace view business_rating_report as
select
    b.name,
    t.min_recorded,
    t.max_recorded,
    p.precipitation,
    r.stars
from
    fact f
    join business_dimention b on f.business_dimention_id = b.id
    join temperature_dimention t on f.temperature_id = t.temperature_id
    join precipitation_dimention p on f.precipitation_id = p.precipitation_id
    join review_dimention r on f.review_dimention_id = r.id
where
    b.city = 'Pittsburgh'
    and b.state = 'PA';

create
or replace view weather_impact_analysis as
select
    stars,
    avg(min_recorded),
    avg(max_recorded),
    avg(precipitation)
from
    business_rating_report
group by
    stars
order by
    stars;
